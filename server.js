// ============================================
// 🚀 خادم Battle Tanks - النسخة النهائية
// ============================================
// نظام: غرفة واحدة من كل نوع (المبتدئين، المتقدمين، المحترفين)
// عند اكتمال الغرفة → تبدأ المعركة → تنشأ غرفة جديدة
// ============================================

require('dotenv').config();
const express = require('express');
const http = require('http');
const socketIo = require('socket.io');
const cors = require('cors');
const helmet = require('helmet');
const compression = require('compression');
const rateLimit = require('express-rate-limit');
const { Pool } = require('pg');
const { Mutex } = require('async-mutex');
const jwt = require('jsonwebtoken');
const path = require('path');

// ============================================
// 📦 المتغيرات البيئية
// ============================================
const {
    DATABASE_URL,
    PORT = 3000,
    JWT_SECRET = 'battle_tanks_secret_2026',
    ADMIN_SECRET = 'admin_secret_2026',
    NODE_ENV = 'production',
    ADMIN_TELEGRAM_ID = '123456789',
    GAME_DURATION = 300,
    REWARD_AMOUNT = 10,
    DEFAULT_BALANCE = 100
} = process.env;

// ============================================
// 🌐 Express
// ============================================
const app = express();

app.use(helmet({ contentSecurityPolicy: false }));
app.use(compression());
app.use(cors({ origin: '*', credentials: true }));

const limiter = rateLimit({
    windowMs: 15 * 60 * 1000,
    max: 100,
    message: { error: 'Too many requests, please try again later.' }
});
app.use('/api/', limiter);

app.use(express.json({ limit: '10mb' }));
app.use(express.static('.'));

// ============================================
// 🗄️ PostgreSQL - اتصال متين
// ============================================
const pool = new Pool({
    connectionString: DATABASE_URL,
    max: 20,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 5000,
    ssl: NODE_ENV === 'production' ? { rejectUnauthorized: false } : false
});

pool.on('connect', () => console.log('✅ PostgreSQL connected'));
pool.on('error', (err) => {
    console.error('❌ PostgreSQL error:', err.message);
    setTimeout(() => pool.connect().catch(() => {}), 5000);
});

// ============================================
// 🔒 أقفال الطوابير (Mutex)
// ============================================
const mutexes = new Map();

function getMutex(key) {
    if (!mutexes.has(key)) {
        mutexes.set(key, new Mutex());
    }
    return mutexes.get(key);
}

// ============================================
// 🏠 إعدادات الغرف
// ============================================
const ROOM_TYPES = [
    { name: 'غرفة المبتدئين', maxSeats: 2, seatPrice: 1, prefix: 'beginner' },
    { name: 'غرفة المتقدمين', maxSeats: 4, seatPrice: 5, prefix: 'advanced' },
    { name: 'غرفة المحترفين', maxSeats: 6, seatPrice: 10, prefix: 'pro' }
];

// الكاش
const roomsCache = new Map();
const playerCache = new Map();
const gameTimers = new Map();
const activeRoomIds = new Set();

// ============================================
// 🗄️ تهيئة قاعدة البيانات (بالترتيب الصحيح)
// ============================================
const INIT_SQL = `
-- 1. جدول المستخدمين
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    telegram_id BIGINT UNIQUE NOT NULL,
    username VARCHAR(100),
    email VARCHAR(255) UNIQUE,
    balance INTEGER DEFAULT 100 NOT NULL,
    is_admin BOOLEAN DEFAULT FALSE,
    games_played INTEGER DEFAULT 0,
    wins INTEGER DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    last_active TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 2. جدول الغرف
CREATE TABLE IF NOT EXISTS rooms (
    id SERIAL PRIMARY KEY,
    room_id VARCHAR(50) UNIQUE NOT NULL,
    room_name VARCHAR(100) NOT NULL,
    max_seats INTEGER NOT NULL DEFAULT 2,
    seat_price INTEGER NOT NULL DEFAULT 1,
    status VARCHAR(20) DEFAULT 'waiting' CHECK (status IN ('waiting', 'active', 'ended')),
    type_name VARCHAR(50) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    start_time TIMESTAMP WITH TIME ZONE,
    end_time TIMESTAMP WITH TIME ZONE,
    version INTEGER DEFAULT 1
);

-- 3. جدول اللاعبين في الغرف
CREATE TABLE IF NOT EXISTS room_players (
    id SERIAL PRIMARY KEY,
    room_id VARCHAR(50) NOT NULL,
    user_id INTEGER NOT NULL,
    socket_id VARCHAR(100),
    team INTEGER DEFAULT 1 CHECK (team IN (1, 2)),
    health INTEGER DEFAULT 100 CHECK (health >= 0 AND health <= 100),
    paid_amount INTEGER DEFAULT 0,
    position_x FLOAT DEFAULT 0,
    position_z FLOAT DEFAULT 0,
    rotation FLOAT DEFAULT 0,
    joined_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    eliminated_at TIMESTAMP WITH TIME ZONE,
    UNIQUE(room_id, user_id)
);

-- 4. إضافة المفاتيح الخارجية
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.table_constraints 
                   WHERE constraint_name = 'fk_room_players_room') THEN
        ALTER TABLE room_players ADD CONSTRAINT fk_room_players_room 
            FOREIGN KEY (room_id) REFERENCES rooms(room_id) ON DELETE CASCADE;
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM information_schema.table_constraints 
                   WHERE constraint_name = 'fk_room_players_user') THEN
        ALTER TABLE room_players ADD CONSTRAINT fk_room_players_user 
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE;
    END IF;
END $$;

-- 5. جدول المعاملات
CREATE TABLE IF NOT EXISTS transactions (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
    amount INTEGER NOT NULL,
    balance_after INTEGER NOT NULL,
    type VARCHAR(30) NOT NULL CHECK (type IN ('deposit', 'withdraw', 'game_fee', 'reward', 'admin', 'refund')),
    description TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 6. جدول المباريات
CREATE TABLE IF NOT EXISTS games (
    id SERIAL PRIMARY KEY,
    room_id VARCHAR(50) REFERENCES rooms(room_id) ON DELETE CASCADE,
    winner_team INTEGER CHECK (winner_team IN (1, 2, 0)),
    winner_user_id INTEGER REFERENCES users(id) ON DELETE SET NULL,
    duration INTEGER,
    total_players INTEGER DEFAULT 0,
    started_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    ended_at TIMESTAMP WITH TIME ZONE,
    status VARCHAR(20) DEFAULT 'started' CHECK (status IN ('started', 'ended', 'aborted'))
);

-- 7. جدول الجلسات
CREATE TABLE IF NOT EXISTS sessions (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
    socket_id VARCHAR(100),
    session_token VARCHAR(255) UNIQUE,
    ip_address VARCHAR(45),
    user_agent TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMP WITH TIME ZONE,
    is_active BOOLEAN DEFAULT TRUE
);

-- 8. جدول سجلات التدقيق
CREATE TABLE IF NOT EXISTS audit_logs (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id) ON DELETE SET NULL,
    action VARCHAR(100) NOT NULL,
    details JSONB,
    ip_address VARCHAR(45),
    user_agent TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- المؤشرات
CREATE INDEX IF NOT EXISTS idx_users_telegram_id ON users(telegram_id);
CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
CREATE INDEX IF NOT EXISTS idx_rooms_status ON rooms(status);
CREATE INDEX IF NOT EXISTS idx_rooms_type_name ON rooms(type_name);
CREATE INDEX IF NOT EXISTS idx_room_players_room_id ON room_players(room_id);
CREATE INDEX IF NOT EXISTS idx_room_players_user_id ON room_players(user_id);
CREATE INDEX IF NOT EXISTS idx_transactions_user_id ON transactions(user_id);
CREATE INDEX IF NOT EXISTS idx_games_room_id ON games(room_id);
CREATE INDEX IF NOT EXISTS idx_games_status ON games(status);

-- دالة تحديث الوقت
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

DROP TRIGGER IF EXISTS update_users_updated_at ON users;
CREATE TRIGGER update_users_updated_at
    BEFORE UPDATE ON users
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();
`;

// ============================================
// 🏠 دوال الغرف
// ============================================

// تهيئة الغرف - غرفة واحدة من كل نوع
async function initRooms() {
    for (const type of ROOM_TYPES) {
        const roomId = `${type.prefix}_room_1`;
        const roomName = `${type.name}`;
        
        const result = await pool.query(
            'SELECT * FROM rooms WHERE room_id = $1',
            [roomId]
        );
        
        if (result.rows.length === 0) {
            await pool.query(
                `INSERT INTO rooms (room_id, room_name, max_seats, seat_price, type_name, status)
                 VALUES ($1, $2, $3, $4, $5, 'waiting')`,
                [roomId, roomName, type.maxSeats, type.seatPrice, type.name]
            );
            console.log(`🏠 Created room: ${roomName}`);
        } else {
            await pool.query(
                `UPDATE rooms 
                 SET max_seats = $1, seat_price = $2, status = 'waiting'
                 WHERE room_id = $3`,
                [type.maxSeats, type.seatPrice, roomId]
            );
            console.log(`🔄 Updated room: ${roomName}`);
        }
        
        roomsCache.set(roomId, {
            id: roomId,
            name: roomName,
            maxSeats: type.maxSeats,
            seatPrice: type.seatPrice,
            typeName: type.name,
            status: 'waiting',
            players: [],
            version: 1
        });
        
        activeRoomIds.add(roomId);
    }
    console.log(`✅ ${roomsCache.size} rooms initialized (one of each type)`);
}

// إنشاء غرفة جديدة
async function createNewRoom(typeName) {
    const type = ROOM_TYPES.find(t => t.name === typeName);
    if (!type) return null;
    
    const mutex = getMutex(`create_room_${typeName}`);
    return await mutex.runExclusive(async () => {
        const result = await pool.query(
            `SELECT room_id FROM rooms 
             WHERE type_name = $1 AND room_id LIKE $2
             ORDER BY room_id DESC LIMIT 1`,
            [typeName, `${type.prefix}_room_%`]
        );
        
        let nextNumber = 1;
        if (result.rows.length > 0) {
            const lastId = result.rows[0].room_id;
            const match = lastId.match(/_room_(\d+)$/);
            if (match) nextNumber = parseInt(match[1]) + 1;
        }
        
        const roomId = `${type.prefix}_room_${nextNumber}`;
        const roomName = `${type.name}`;
        
        await pool.query('DELETE FROM rooms WHERE room_id = $1', [roomId]);
        await pool.query(
            `INSERT INTO rooms (room_id, room_name, max_seats, seat_price, type_name, status)
             VALUES ($1, $2, $3, $4, $5, 'waiting')`,
            [roomId, roomName, type.maxSeats, type.seatPrice, type.name]
        );
        
        const newRoom = {
            id: roomId,
            name: roomName,
            maxSeats: type.maxSeats,
            seatPrice: type.seatPrice,
            typeName: type.name,
            status: 'waiting',
            players: [],
            version: Date.now()
        };
        
        for (const [oldId, room] of roomsCache) {
            if (room.typeName === typeName && oldId !== roomId) {
                roomsCache.delete(oldId);
                activeRoomIds.delete(oldId);
            }
        }
        
        roomsCache.set(roomId, newRoom);
        activeRoomIds.add(roomId);
        return newRoom;
    });
}

// الحصول على الغرف المتاحة
async function getAvailableRooms() {
    const rooms = [];
    for (const type of ROOM_TYPES) {
        const result = await pool.query(
            `SELECT r.*, COUNT(rp.id) as player_count
             FROM rooms r
             LEFT JOIN room_players rp ON r.room_id = rp.room_id
             WHERE r.type_name = $1 AND r.status = 'waiting'
             GROUP BY r.id
             ORDER BY r.created_at DESC
             LIMIT 1`,
            [type.name]
        );
        
        if (result.rows.length > 0) {
            const row = result.rows[0];
            const playerCount = parseInt(row.player_count) || 0;
            rooms.push({
                id: row.room_id,
                name: row.room_name,
                players: playerCount,
                maxSeats: row.max_seats,
                seatPrice: row.seat_price,
                status: row.status,
                typeName: row.type_name,
                needed: row.max_seats - playerCount,
                isFull: playerCount >= row.max_seats
            });
        } else {
            const newRoom = await createNewRoom(type.name);
            if (newRoom) {
                rooms.push({
                    id: newRoom.id,
                    name: newRoom.name,
                    players: 0,
                    maxSeats: newRoom.maxSeats,
                    seatPrice: newRoom.seatPrice,
                    status: 'waiting',
                    typeName: newRoom.typeName,
                    needed: newRoom.maxSeats,
                    isFull: false
                });
            }
        }
    }
    return rooms;
}

// ============================================
// 👤 دوال المستخدمين
// ============================================

async function getUser(telegramId) {
    const result = await pool.query(
        'SELECT * FROM users WHERE telegram_id = $1',
        [telegramId]
    );
    return result.rows[0] || null;
}

async function createUser(telegramId, username = null) {
    const result = await pool.query(
        `INSERT INTO users (telegram_id, username, balance)
         VALUES ($1, $2, $3) RETURNING *`,
        [telegramId, username || `user_${telegramId}`, DEFAULT_BALANCE]
    );
    return result.rows[0];
}

async function getOrCreateUser(telegramId) {
    let user = await getUser(telegramId);
    if (!user) {
        user = await createUser(telegramId);
        console.log(`👤 New user: ${telegramId}`);
    }
    return user;
}

// ============================================
// 🎮 دوال اللعبة
// ============================================

// الانضمام إلى غرفة
async function joinRoom(socket, userId, roomId) {
    const roomMutex = getMutex(`room_${roomId}`);
    const userMutex = getMutex(`user_${userId}`);
    
    return await roomMutex.runExclusive(async () => {
        return await userMutex.runExclusive(async () => {
            const existing = await pool.query(
                'SELECT * FROM room_players WHERE room_id = $1 AND user_id = $2',
                [roomId, userId]
            );
            if (existing.rows.length > 0) {
                return { success: false, error: 'أنت بالفعل في هذه الغرفة' };
            }
            
            const roomResult = await pool.query(
                'SELECT * FROM rooms WHERE room_id = $1 AND status = $2',
                [roomId, 'waiting']
            );
            if (roomResult.rows.length === 0) {
                return { success: false, error: 'الغرفة غير متاحة' };
            }
            const room = roomResult.rows[0];
            
            const countResult = await pool.query(
                'SELECT COUNT(*) FROM room_players WHERE room_id = $1',
                [roomId]
            );
            const playerCount = parseInt(countResult.rows[0].count);
            if (playerCount >= room.max_seats) {
                return { success: false, error: 'الغرفة مكتملة' };
            }
            
            const userResult = await pool.query(
                'SELECT balance FROM users WHERE id = $1',
                [userId]
            );
            const balance = userResult.rows[0]?.balance || DEFAULT_BALANCE;
            const seatPrice = room.seat_price || 1;
            
            if (balance < seatPrice) {
                return { success: false, error: `رصيد غير كافٍ! المطلوب: ${seatPrice}$` };
            }
            
            await pool.query(
                'UPDATE users SET balance = balance - $1 WHERE id = $2',
                [seatPrice, userId]
            );
            
            await pool.query(
                `INSERT INTO transactions (user_id, amount, balance_after, type, description)
                 SELECT $1, -$2, balance, 'game_fee', 'رسوم دخول الغرفة'
                 FROM users WHERE id = $1`,
                [userId, seatPrice]
            );
            
            await pool.query(
                `INSERT INTO room_players (room_id, user_id, socket_id, paid_amount, team, health)
                 VALUES ($1, $2, $3, $4, $5, $6)`,
                [roomId, userId, socket.id, seatPrice, 1, 100]
            );
            
            const newCount = await pool.query(
                'SELECT COUNT(*) FROM room_players WHERE room_id = $1',
                [roomId]
            );
            const newPlayerCount = parseInt(newCount.rows[0].count);
            
            const isFull = newPlayerCount >= room.max_seats;
            if (isFull) {
                setTimeout(() => startGame(roomId), 1000);
            }
            
            return {
                success: true,
                balance: balance - seatPrice,
                playersCount: newPlayerCount,
                maxSeats: room.max_seats,
                seatPrice: seatPrice,
                needed: room.max_seats - newPlayerCount,
                roomName: room.room_name,
                isFull: isFull
            };
        });
    });
}

// بدء المعركة
async function startGame(roomId) {
    const mutex = getMutex(`room_${roomId}`);
    
    await mutex.runExclusive(async () => {
        const roomCheck = await pool.query(
            'SELECT status, max_seats, type_name FROM rooms WHERE room_id = $1',
            [roomId]
        );
        if (roomCheck.rows.length === 0 || roomCheck.rows[0].status !== 'waiting') {
            return;
        }
        
        const typeName = roomCheck.rows[0].type_name;
        
        await pool.query(
            `UPDATE rooms SET status = 'active', start_time = CURRENT_TIMESTAMP WHERE room_id = $1`,
            [roomId]
        );
        
        const playersResult = await pool.query(`
            SELECT rp.*, u.username, u.telegram_id
            FROM room_players rp
            JOIN users u ON rp.user_id = u.id
            WHERE rp.room_id = $1
        `, [roomId]);
        
        const players = playersResult.rows;
        const positions = [
            { x: -120, z: -80, team: 1 },
            { x: 120, z: 80, team: 2 }
        ];
        
        for (let i = 0; i < players.length; i++) {
            const pos = positions[i % positions.length];
            await pool.query(
                `UPDATE room_players 
                 SET team = $1, position_x = $2, position_z = $3
                 WHERE room_id = $4 AND user_id = $5`,
                [pos.team, pos.x, pos.z, roomId, players[i].user_id]
            );
        }
        
        const playersData = players.map(p => ({ userId: p.user_id, team: p.team }));
        
        for (const player of players) {
            const socket = io.sockets.sockets.get(player.socket_id);
            if (socket) {
                socket.emit('game_start', {
                    roomId: roomId,
                    players: playersData,
                    yourTeam: player.team,
                    position: { x: player.position_x, z: player.position_z },
                    health: player.health || 100,
                    roomType: typeName
                });
            }
        }
        
        console.log(`🎮 Game started: ${roomId} (${players.length} players) - ${typeName}`);
        
        await pool.query(
            `INSERT INTO games (room_id, total_players, status, started_at)
             VALUES ($1, $2, 'started', CURRENT_TIMESTAMP)`,
            [roomId, players.length]
        );
        
        if (gameTimers.has(roomId)) {
            clearTimeout(gameTimers.get(roomId));
            gameTimers.delete(roomId);
        }
        
        const timer = setTimeout(() => endGame(roomId), (GAME_DURATION || 300) * 1000);
        gameTimers.set(roomId, timer);
        
        const rooms = await getAvailableRooms();
        io.emit('rooms_list', { rooms });
    });
}

// إنهاء المعركة
async function endGame(roomId) {
    const mutex = getMutex(`room_${roomId}`);
    
    await mutex.runExclusive(async () => {
        const roomCheck = await pool.query(
            'SELECT status, type_name FROM rooms WHERE room_id = $1',
            [roomId]
        );
        if (roomCheck.rows.length === 0 || roomCheck.rows[0].status !== 'active') {
            return;
        }
        
        const typeName = roomCheck.rows[0].type_name;
        
        if (gameTimers.has(roomId)) {
            clearTimeout(gameTimers.get(roomId));
            gameTimers.delete(roomId);
        }
        
        await pool.query(
            `UPDATE rooms SET status = 'ended', end_time = CURRENT_TIMESTAMP WHERE room_id = $1`,
            [roomId]
        );
        
        const aliveResult = await pool.query(`
            SELECT rp.*, u.id as user_id, u.balance
            FROM room_players rp
            JOIN users u ON rp.user_id = u.id
            WHERE rp.room_id = $1 AND rp.health > 0
        `, [roomId]);
        
        const alive = aliveResult.rows;
        let winnerTeam = null;
        let winnerUserId = null;
        
        if (alive.length === 1) {
            winnerTeam = alive[0].team;
            winnerUserId = alive[0].user_id;
        } else if (alive.length >= 2) {
            const p1 = alive[0], p2 = alive[1];
            if (p1.health > p2.health) {
                winnerTeam = p1.team;
                winnerUserId = p1.user_id;
            } else if (p2.health > p1.health) {
                winnerTeam = p2.team;
                winnerUserId = p2.user_id;
            }
        }
        
        await pool.query(
            `UPDATE games 
             SET winner_team = $1, winner_user_id = $2, 
                 ended_at = CURRENT_TIMESTAMP, status = 'ended',
                 duration = EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - started_at))::INTEGER
             WHERE room_id = $3 AND status = 'started'`,
            [winnerTeam || 0, winnerUserId || null, roomId]
        );
        
        const rewardAmount = parseInt(REWARD_AMOUNT) || 10;
        for (const player of alive) {
            const isWinner = (player.team === winnerTeam && winnerTeam !== null);
            const reward = isWinner ? rewardAmount : 0;
            
            if (reward > 0) {
                await pool.query(
                    `UPDATE users 
                     SET balance = balance + $1, games_played = games_played + 1, wins = wins + 1
                     WHERE id = $2`,
                    [reward, player.user_id]
                );
                
                await pool.query(
                    `INSERT INTO transactions (user_id, amount, balance_after, type, description)
                     SELECT $1, $2, balance, 'reward', 'مكافأة الفوز في المعركة'
                     FROM users WHERE id = $1`,
                    [player.user_id, reward]
                );
            } else {
                await pool.query(
                    `UPDATE users SET games_played = games_played + 1 WHERE id = $1`,
                    [player.user_id]
                );
            }
            
            const socket = io.sockets.sockets.get(player.socket_id);
            if (socket) {
                socket.emit('game_ended', {
                    roomId: roomId,
                    winner: isWinner ? '🎉 فوز!' : (winnerTeam ? '💪 أحسنت!' : '🤝 تعادل'),
                    reward: reward,
                    yourTeam: player.team === 1 ? 'فريقك' : 'الفريق الخصم',
                    yourBalance: player.balance + reward,
                    health: player.health,
                    message: isWinner ? `🎉 لقد فزت بالمعركة! +${reward}$` : (winnerTeam ? '💪 معركة جيدة!' : '🤝 تعادل!')
                });
            }
        }
        
        console.log(`🏆 Game ended: ${roomId}, winner: ${winnerTeam || 'draw'} (${typeName})`);
        
        await pool.query('DELETE FROM room_players WHERE room_id = $1', [roomId]);
        await pool.query('DELETE FROM rooms WHERE room_id = $1', [roomId]);
        
        roomsCache.delete(roomId);
        activeRoomIds.delete(roomId);
        
        console.log(`🧹 Room ${roomId} deleted`);
        
        const newRoom = await createNewRoom(typeName);
        if (newRoom) {
            console.log(`🔄 New room created: ${newRoom.name} (${newRoom.id})`);
        }
        
        const rooms = await getAvailableRooms();
        io.emit('rooms_list', { rooms });
    });
}

// ============================================
// 🌐 HTTP Routes
// ============================================

app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'index.html'));
});

app.get('/admin', (req, res) => {
    res.sendFile(path.join(__dirname, 'admin.html'));
});

app.get('/game', (req, res) => {
    res.sendFile(path.join(__dirname, 'game.html'));
});

app.get('/health', async (req, res) => {
    try {
        await pool.query('SELECT 1');
        res.json({ 
            status: 'ok', 
            timestamp: Date.now(),
            rooms: roomsCache.size,
            players: playerCache.size,
            activeRooms: Array.from(activeRoomIds)
        });
    } catch (error) {
        res.status(500).json({ status: 'error', error: error.message });
    }
});

app.get('/api/rooms', async (req, res) => {
    try {
        const rooms = await getAvailableRooms();
        res.json({ success: true, rooms });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

app.get('/api/user/:telegramId', async (req, res) => {
    try {
        const user = await getUser(req.params.telegramId);
        if (!user) {
            return res.status(404).json({ success: false, error: 'User not found' });
        }
        res.json({ success: true, user });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

// ============================================
// 🛠️ API Routes - الإدارة
// ============================================

app.get('/api/admin/stats', async (req, res) => {
    try {
        if (req.query.adminToken !== ADMIN_SECRET) {
            return res.status(403).json({ success: false, error: 'Unauthorized' });
        }
        const result = await pool.query(`
            SELECT 
                (SELECT COUNT(*) FROM users) as users,
                (SELECT SUM(balance) FROM users) as total_balance,
                (SELECT COUNT(*) FROM rooms WHERE status = 'active') as active_rooms,
                (SELECT COUNT(*) FROM rooms WHERE status = 'waiting') as waiting_rooms,
                (SELECT COUNT(*) FROM games WHERE status = 'ended') as total_games
        `);
        res.json({ success: true, stats: result.rows[0] });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

app.post('/api/admin/balance', async (req, res) => {
    try {
        const { adminToken, userId, amount, action } = req.body;
        
        if (adminToken !== ADMIN_SECRET) {
            return res.status(403).json({ success: false, error: 'Unauthorized' });
        }
        
        if (!userId || !amount || amount < 1) {
            return res.status(400).json({ success: false, error: 'Invalid input' });
        }
        
        const sign = action === 'deposit' ? '+' : '-';
        const type = action === 'deposit' ? 'deposit' : 'withdraw';
        
        const result = await pool.query(
            `UPDATE users SET balance = balance ${sign} $1 WHERE telegram_id = $2 RETURNING balance`,
            [amount, userId]
        );
        
        if (result.rows.length === 0) {
            return res.status(404).json({ success: false, error: 'User not found' });
        }
        
        await pool.query(
            `INSERT INTO transactions (user_id, amount, balance_after, type, description)
             SELECT id, $1, balance, $2, $3
             FROM users WHERE telegram_id = $4`,
            [action === 'deposit' ? amount : -amount, type, `عملية ${action} بواسطة المشرف`, userId]
        );
        
        res.json({ success: true, newBalance: result.rows[0].balance });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

app.post('/api/admin/roomType', async (req, res) => {
    try {
        const { adminToken, typeName, maxSeats, seatPrice } = req.body;
        
        if (adminToken !== ADMIN_SECRET) {
            return res.status(403).json({ success: false, error: 'Unauthorized' });
        }
        
        const newMaxSeats = Math.max(2, Math.min(16, maxSeats || 2));
        const newSeatPrice = Math.max(1, Math.min(1000, seatPrice || 1));
        
        const type = ROOM_TYPES.find(t => t.name === typeName);
        if (type) {
            type.maxSeats = newMaxSeats;
            type.seatPrice = newSeatPrice;
        }
        
        for (const [id, room] of roomsCache) {
            if (room.typeName === typeName) {
                room.maxSeats = newMaxSeats;
                room.seatPrice = newSeatPrice;
                roomsCache.set(id, room);
                
                await pool.query(
                    `UPDATE rooms SET max_seats = $1, seat_price = $2 WHERE room_id = $3`,
                    [newMaxSeats, newSeatPrice, id]
                );
                break;
            }
        }
        
        res.json({ success: true, message: `تم تحديث ${typeName}`, maxSeats: newMaxSeats, seatPrice: newSeatPrice });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

app.post('/api/admin/resetAll', async (req, res) => {
    try {
        const { adminToken } = req.body;
        
        if (adminToken !== ADMIN_SECRET) {
            return res.status(403).json({ success: false, error: 'Unauthorized' });
        }
        
        await pool.query('DELETE FROM room_players');
        await pool.query('DELETE FROM games');
        await pool.query('DELETE FROM transactions');
        await pool.query('UPDATE rooms SET status = $1, start_time = NULL, end_time = NULL', ['waiting']);
        await pool.query(
            `UPDATE users SET balance = $1, games_played = 0, wins = 0,
             is_admin = CASE WHEN telegram_id = $2 THEN true ELSE false END`,
            [DEFAULT_BALANCE, ADMIN_TELEGRAM_ID]
        );
        
        playerCache.clear();
        for (const [id, room] of roomsCache) {
            room.status = 'waiting';
            room.players = [];
            roomsCache.set(id, room);
        }
        
        await initRooms();
        res.json({ success: true, message: 'تم مسح جميع البيانات' });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

// ============================================
// 🔌 Socket.io
// ============================================
const server = http.createServer(app);
const io = socketIo(server, {
    cors: { origin: "*", methods: ["GET", "POST"] },
    transports: ['websocket', 'polling'],
    pingTimeout: 60000,
    pingInterval: 25000
});

io.on('connection', (socket) => {
    console.log(`🔌 Connected: ${socket.id}`);
    
    socket.on('ping', () => {
        socket.emit('pong', { timestamp: Date.now() });
    });
    
    // المصادقة
    socket.on('auth', async (data) => {
        try {
            const { telegramId } = data || {};
            
            if (!telegramId) {
                socket.emit('auth_error', { message: 'Telegram ID required' });
                return;
            }
            
            const user = await getOrCreateUser(telegramId);
            
            const token = jwt.sign(
                { telegramId, userId: user.id },
                JWT_SECRET,
                { expiresIn: '7d' }
            );
            
            playerCache.set(socket.id, {
                socketId: socket.id,
                userId: user.id,
                telegramId: user.telegram_id,
                username: user.username,
                isAdmin: user.is_admin || false,
                balance: user.balance || DEFAULT_BALANCE,
                roomId: null
            });
            
            socket.emit('auth_success', {
                userId: user.id,
                telegramId: user.telegram_id,
                username: user.username,
                email: user.email,
                balance: user.balance || DEFAULT_BALANCE,
                isAdmin: user.is_admin || false,
                gamesPlayed: user.games_played || 0,
                wins: user.wins || 0,
                token: token
            });
            
            const rooms = await getAvailableRooms();
            socket.emit('lobby_joined', { rooms });
            
            console.log(`✅ Auth: ${user.username} (${telegramId})`);
        } catch (error) {
            console.error('Auth error:', error);
            socket.emit('auth_error', { message: error.message });
        }
    });
    
    // اللوبي
    socket.on('join_lobby', async () => {
        const player = playerCache.get(socket.id);
        if (!player) {
            socket.emit('error', { message: 'Not authenticated' });
            return;
        }
        
        try {
            const rooms = await getAvailableRooms();
            socket.emit('lobby_joined', { 
                rooms,
                balance: player.balance,
                userId: player.userId,
                isAdmin: player.isAdmin
            });
        } catch (error) {
            socket.emit('error', { message: error.message });
        }
    });
    
    // الانضمام إلى غرفة
    socket.on('join_room', async (data) => {
        const player = playerCache.get(socket.id);
        if (!player) {
            socket.emit('error', { message: 'Not authenticated' });
            return;
        }
        
        const { roomId } = data;
        if (!roomId) {
            socket.emit('error', { message: 'Room ID required' });
            return;
        }
        
        try {
            const result = await joinRoom(socket, player.userId, roomId);
            
            if (!result.success) {
                socket.emit('error', { message: result.error });
                return;
            }
            
            player.balance = result.balance;
            player.roomId = roomId;
            playerCache.set(socket.id, player);
            
            socket.join(roomId);
            
            socket.emit('room_joined', {
                roomId: roomId,
                roomName: result.roomName,
                balance: result.balance,
                playersCount: result.playersCount,
                maxSeats: result.maxSeats,
                needed: result.needed,
                isFull: result.isFull
            });
            
            io.to(roomId).emit('player_joined', {
                userId: player.userId,
                username: player.username,
                playersCount: result.playersCount,
                maxSeats: result.maxSeats,
                needed: result.needed
            });
            
            const rooms = await getAvailableRooms();
            io.emit('rooms_list', { rooms });
            
            console.log(`👥 ${player.username} joined ${roomId} (${result.playersCount}/${result.maxSeats})`);
        } catch (error) {
            console.error('Join room error:', error);
            socket.emit('error', { message: error.message });
        }
    });
    
    // أحداث اللعبة
    socket.on('move', (data) => {
        const player = playerCache.get(socket.id);
        if (player && player.roomId) {
            socket.to(player.roomId).emit('player_moved', {
                userId: player.userId,
                position: data.position,
                rotation: data.rotation
            });
        }
    });
    
    socket.on('shoot', (data) => {
        const player = playerCache.get(socket.id);
        if (player && player.roomId) {
            socket.to(player.roomId).emit('player_shot', {
                userId: player.userId,
                position: data.position,
                direction: data.direction
            });
        }
    });
    
    socket.on('damage', async (data) => {
        const player = playerCache.get(socket.id);
        if (!player || !player.roomId) return;
        
        try {
            const { targetUserId, damage } = data;
            
            const result = await pool.query(
                `UPDATE room_players 
                 SET health = GREATEST(health - $1, 0)
                 WHERE room_id = (SELECT room_id FROM room_players WHERE socket_id = $2)
                 AND user_id = $3
                 RETURNING health`,
                [damage || 100, socket.id, targetUserId]
            );
            
            if (result.rows.length > 0) {
                const health = result.rows[0].health;
                
                io.to(player.roomId).emit('health_update', {
                    userId: targetUserId,
                    health: health
                });
                
                if (health <= 0) {
                    await pool.query(
                        `UPDATE room_players 
                         SET eliminated_at = CURRENT_TIMESTAMP 
                         WHERE room_id = (SELECT room_id FROM room_players WHERE socket_id = $1)
                         AND user_id = $2`,
                        [socket.id, targetUserId]
                    );
                    
                    io.to(player.roomId).emit('player_eliminated', {
                        userId: targetUserId,
                        killerId: player.userId
                    });
                    
                    const targetSocket = io.sockets.sockets.get(socket.id);
                    if (targetSocket) {
                        targetSocket.emit('you_were_eliminated', {
                            killerId: player.userId,
                            message: '💀 لقد تم تدمير دبابتك!'
                        });
                    }
                    
                    const alive = await pool.query(
                        `SELECT COUNT(*) 
                         FROM room_players 
                         WHERE room_id = (SELECT room_id FROM room_players WHERE socket_id = $1)
                         AND health > 0`,
                        [socket.id]
                    );
                    
                    if (parseInt(alive.rows[0].count) <= 1) {
                        setTimeout(() => endGame(player.roomId), 1500);
                    }
                }
            }
        } catch (error) {
            console.error('Damage error:', error);
        }
    });
    
    // مغادرة الغرفة
    socket.on('leave_room', async () => {
        const player = playerCache.get(socket.id);
        if (!player || !player.roomId) {
            socket.emit('error', { message: 'You are not in any room' });
            return;
        }
        
        try {
            const roomId = player.roomId;
            
            const roomCheck = await pool.query(
                'SELECT status FROM rooms WHERE room_id = $1',
                [roomId]
            );
            
            if (roomCheck.rows.length > 0 && roomCheck.rows[0].status === 'active') {
                socket.emit('error', { message: 'لا يمكن مغادرة الغرفة أثناء المعركة' });
                return;
            }
            
            await pool.query(
                'DELETE FROM room_players WHERE socket_id = $1',
                [socket.id]
            );
            
            socket.leave(roomId);
            player.roomId = null;
            playerCache.set(socket.id, player);
            
            io.to(roomId).emit('player_left', {
                userId: player.userId,
                username: player.username
            });
            
            const rooms = await getAvailableRooms();
            io.emit('rooms_list', { rooms });
            
            socket.emit('room_left', { success: true });
        } catch (error) {
            socket.emit('error', { message: error.message });
        }
    });
    
    // قطع الاتصال
    socket.on('disconnect', async () => {
        const player = playerCache.get(socket.id);
        if (player) {
            console.log(`🔌 Disconnected: ${player.username}`);
            
            if (player.roomId) {
                try {
                    const roomCheck = await pool.query(
                        'SELECT status FROM rooms WHERE room_id = $1',
                        [player.roomId]
                    );
                    
                    if (roomCheck.rows.length > 0 && roomCheck.rows[0].status === 'waiting') {
                        await pool.query(
                            'DELETE FROM room_players WHERE socket_id = $1',
                            [socket.id]
                        );
                        socket.to(player.roomId).emit('player_left', {
                            userId: player.userId
                        });
                    }
                } catch (error) {
                    console.error('Disconnect error:', error);
                }
            }
            
            playerCache.delete(socket.id);
        }
    });
});

// ============================================
// 🚀 تشغيل الخادم
// ============================================

async function start() {
    try {
        // 1. تهيئة قاعدة البيانات
        await pool.query(INIT_SQL);
        console.log('✅ Database tables ready');
        
        // 2. إنشاء المستخدم المشرف
        const adminResult = await pool.query(
            'SELECT * FROM users WHERE telegram_id = $1',
            [ADMIN_TELEGRAM_ID]
        );
        if (adminResult.rows.length === 0) {
            await pool.query(
                `INSERT INTO users (telegram_id, username, email, balance, is_admin)
                 VALUES ($1, $2, $3, $4, $5)`,
                [ADMIN_TELEGRAM_ID, 'Admin', 'admin@battletanks.com', 9999, true]
            );
            console.log('👑 Admin user created');
        }
        
        // 3. تهيئة الغرف
        await initRooms();
        
        // 4. تشغيل الخادم
        server.listen(PORT, () => {
            console.log(`
╔═══════════════════════════════════════════════════════════════════╗
║                    🎮 BATTLE TANKS SERVER 🎮                     ║
╠═══════════════════════════════════════════════════════════════════╣
║  📡 Port: ${PORT}                                                  ║
║  🗄️ Database: PostgreSQL (${NODE_ENV})                            ║
║  🏠 Rooms: ${roomsCache.size} (one of each type)                  ║
║  👑 Admin: Telegram ID ${ADMIN_TELEGRAM_ID}                       ║
╠═══════════════════════════════════════════════════════════════════╣
║  📋 Room Types:                                                  ║
${ROOM_TYPES.map(t => `║     - ${t.name}: ${t.maxSeats} players, ${t.seatPrice}$ per seat`).join('\n║     ')}
╠═══════════════════════════════════════════════════════════════════╣
║  ⚙️ Settings:                                                    ║
║     - Game duration: ${GAME_DURATION}s                            ║
║     - Reward: ${REWARD_AMOUNT}$                                  ║
║     - Default balance: ${DEFAULT_BALANCE}$                       ║
║  🔄 New room created automatically after each match              ║
╚═══════════════════════════════════════════════════════════════════╝
            `);
        });
    } catch (error) {
        console.error('❌ Failed to start:', error.message);
        process.exit(1);
    }
}

// إيقاف آمن
process.on('SIGINT', async () => {
    console.log('\n🛑 Shutting down...');
    await pool.end();
    process.exit(0);
});

process.on('SIGTERM', async () => {
    console.log('\n🛑 Shutting down...');
    await pool.end();
    process.exit(0);
});

// ============================================
// 🚀 بدء التشغيل
// ============================================
start();
