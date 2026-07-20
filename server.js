// ================================================================
// 🚀 BATTLE TANKS ELITE v6.1 - خادم احترافي AAA (مصحح)
// ================================================================
// جميع الحقوق محفوظة © 2026 - النسخة النهائية للإنتاج
// ================================================================

require('dotenv').config();
const express = require('express');
const http = require('http');
const socketIo = require('socket.io');
const { Pool } = require('pg');
const cors = require('cors');
const helmet = require('helmet');
const compression = require('compression');
const rateLimit = require('express-rate-limit');
const slowDown = require('express-slow-down');
const jwt = require('jsonwebtoken');
const { v4: uuidv4 } = require('uuid');
const path = require('path');
const crypto = require('crypto');
const fs = require('fs');
const EventEmitter = require('events');

// ================================================================
// 📝 نظام التسجيل المتقدم
// ================================================================
const winston = require('winston');
const logger = winston.createLogger({
    level: process.env.LOG_LEVEL || 'info',
    format: winston.format.combine(
        winston.format.timestamp(),
        winston.format.errors({ stack: true }),
        winston.format.json()
    ),
    defaultMeta: { service: 'battle-tanks' },
    transports: [
        new winston.transports.Console({
            format: winston.format.combine(
                winston.format.colorize(),
                winston.format.simple()
            )
        })
    ]
});

// ================================================================
// 🗄️ اتصال PostgreSQL (Neon) مع SSL وإعادة محاولة متينة
// ================================================================
const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: { rejectUnauthorized: false },
    max: 100,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 10000,
    maxUses: 7500,
});

// مراقبة الأخطاء وإعادة المحاولة الذكية
let isReconnecting = false;
let reconnectAttempts = 0;
const MAX_RECONNECT_ATTEMPTS = 20;

pool.on('error', async (err) => {
    logger.error('❌ PostgreSQL Pool Error:', err);
    if (err.code === 'ECONNRESET' || err.code === 'PROTOCOL_CONNECTION_LOST' || 
        err.code === 'ETIMEDOUT' || err.code === '57P01') {
        if (!isReconnecting) {
            await reconnectDatabase();
        }
    }
});

async function reconnectDatabase() {
    if (isReconnecting) return;
    isReconnecting = true;

    try {
        await pool.end();
    } catch (e) {}

    while (reconnectAttempts < MAX_RECONNECT_ATTEMPTS) {
        reconnectAttempts++;
        const delay = Math.min(1000 * Math.pow(1.5, reconnectAttempts), 30000);
        logger.info(`🔄 Reconnect attempt ${reconnectAttempts} in ${delay}ms...`);

        await new Promise(resolve => setTimeout(resolve, delay));

        try {
            const newPool = new Pool({
                connectionString: process.env.DATABASE_URL,
                ssl: { rejectUnauthorized: false },
                max: 100,
                idleTimeoutMillis: 30000,
                connectionTimeoutMillis: 10000,
                maxUses: 7500,
            });

            await newPool.query('SELECT 1');
            Object.assign(pool, newPool);
            logger.info('✅ Database reconnected successfully!');
            reconnectAttempts = 0;
            isReconnecting = false;
            return;
        } catch (error) {
            logger.error(`❌ Reconnect attempt ${reconnectAttempts} failed:`, error.message);
        }
    }

    logger.error('❌ Max reconnect attempts reached. Server will continue but database operations may fail.');
    isReconnecting = false;
}

// ================================================================
// 📋 إنشاء قاعدة البيانات والجداول تلقائياً
// ================================================================
async function initializeDatabase() {
    logger.info('📋 Initializing database schema...');

    const queries = [
        // ============================================================
        // تمكين التوسعات
        // ============================================================
        `CREATE EXTENSION IF NOT EXISTS "uuid-ossp";`,
        `CREATE EXTENSION IF NOT EXISTS "pgcrypto";`,

        // ============================================================
        // جدول المستخدمين (بدون عمود email - نستخدم telegram_id فقط)
        // ============================================================
        `
        CREATE TABLE IF NOT EXISTS users (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            telegram_id BIGINT UNIQUE NOT NULL,
            telegram_username VARCHAR(255),
            username VARCHAR(255) UNIQUE,
            balance INTEGER DEFAULT 100 NOT NULL CHECK (balance >= 0),
            is_admin BOOLEAN DEFAULT FALSE,
            is_banned BOOLEAN DEFAULT FALSE,
            is_verified BOOLEAN DEFAULT FALSE,
            games_played INTEGER DEFAULT 0,
            wins INTEGER DEFAULT 0,
            losses INTEGER DEFAULT 0,
            total_kills INTEGER DEFAULT 0,
            total_deaths INTEGER DEFAULT 0,
            longest_streak INTEGER DEFAULT 0,
            current_streak INTEGER DEFAULT 0,
            total_damage_dealt INTEGER DEFAULT 0,
            total_damage_taken INTEGER DEFAULT 0,
            accuracy FLOAT DEFAULT 0,
            favorite_team INTEGER DEFAULT 1,
            favorite_map VARCHAR(50),
            total_playtime INTEGER DEFAULT 0,
            last_active TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_ip INET,
            last_seen_room VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            deleted_at TIMESTAMP
        );
        `,

        // ============================================================
        // جدول الجلسات (مع دعم الأجهزة المتعددة)
        // ============================================================
        `
        CREATE TABLE IF NOT EXISTS sessions (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            token TEXT NOT NULL UNIQUE,
            refresh_token TEXT UNIQUE,
            socket_id VARCHAR(255),
            device_id VARCHAR(255),
            device_name VARCHAR(255),
            device_type VARCHAR(50),
            ip_address INET,
            user_agent TEXT,
            expires_at TIMESTAMP NOT NULL,
            refresh_expires_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_activity TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        `,

        // ============================================================
        // جدول الغرف (مع دعم الإعدادات المتقدمة)
        // ============================================================
        `
        CREATE TABLE IF NOT EXISTS rooms (
            id VARCHAR(50) PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            type VARCHAR(50) NOT NULL,
            max_seats INTEGER NOT NULL DEFAULT 2,
            seat_price INTEGER NOT NULL DEFAULT 1,
            status VARCHAR(20) DEFAULT 'waiting',
            players_count INTEGER DEFAULT 0,
            started_at TIMESTAMP,
            ended_at TIMESTAMP,
            winner_team INTEGER,
            game_data JSONB,
            map_id VARCHAR(50) DEFAULT 'desert',
            map_name VARCHAR(100) DEFAULT 'صحراء',
            game_mode VARCHAR(50) DEFAULT 'team_deathmatch',
            duration INTEGER DEFAULT 300,
            max_score INTEGER DEFAULT 50,
            created_by UUID REFERENCES users(id),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        `,

        // ============================================================
        // جدول مشاركات اللاعبين (مع إحصائيات متقدمة)
        // ============================================================
        `
        CREATE TABLE IF NOT EXISTS room_players (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            room_id VARCHAR(50) NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
            user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            socket_id VARCHAR(255),
            team INTEGER DEFAULT 1,
            health INTEGER DEFAULT 100,
            max_health INTEGER DEFAULT 100,
            paid_amount INTEGER NOT NULL DEFAULT 0,
            position_x FLOAT DEFAULT 0,
            position_y FLOAT DEFAULT 0,
            position_z FLOAT DEFAULT 0,
            rotation FLOAT DEFAULT 0,
            kills INTEGER DEFAULT 0,
            deaths INTEGER DEFAULT 0,
            assists INTEGER DEFAULT 0,
            damage_dealt INTEGER DEFAULT 0,
            damage_taken INTEGER DEFAULT 0,
            shots_fired INTEGER DEFAULT 0,
            shots_hit INTEGER DEFAULT 0,
            accuracy FLOAT DEFAULT 0,
            is_alive BOOLEAN DEFAULT TRUE,
            is_ready BOOLEAN DEFAULT FALSE,
            respawn_timer INTEGER DEFAULT 0,
            joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            left_at TIMESTAMP,
            UNIQUE(room_id, user_id)
        );
        `,

        // ============================================================
        // جدول المعاملات المالية
        // ============================================================
        `
        CREATE TABLE IF NOT EXISTS transactions (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            type VARCHAR(30) NOT NULL,
            amount INTEGER NOT NULL,
            balance_after INTEGER NOT NULL,
            description TEXT,
            room_id VARCHAR(50),
            admin_id UUID REFERENCES users(id),
            reference_id UUID,
            metadata JSONB,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        `,

        // ============================================================
        // جدول سجل التدقيق (Audit Log)
        // ============================================================
        `
        CREATE TABLE IF NOT EXISTS audit_logs (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id UUID REFERENCES users(id) ON DELETE SET NULL,
            action VARCHAR(100) NOT NULL,
            details JSONB,
            ip_address INET,
            user_agent TEXT,
            severity VARCHAR(20) DEFAULT 'info',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        `,

        // ============================================================
        // جدول الإحصائيات اليومية
        // ============================================================
        `
        CREATE TABLE IF NOT EXISTS daily_stats (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            date DATE NOT NULL UNIQUE,
            total_users INTEGER DEFAULT 0,
            active_users INTEGER DEFAULT 0,
            new_users INTEGER DEFAULT 0,
            games_played INTEGER DEFAULT 0,
            total_revenue INTEGER DEFAULT 0,
            total_damage INTEGER DEFAULT 0,
            avg_players_per_game FLOAT DEFAULT 0,
            peak_players INTEGER DEFAULT 0,
            avg_game_duration FLOAT DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        `,

        // ============================================================
        // جدول الحظر (Ban List)
        // ============================================================
        `
        CREATE TABLE IF NOT EXISTS bans (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            admin_id UUID REFERENCES users(id),
            reason TEXT,
            duration INTEGER,
            expires_at TIMESTAMP,
            ip_address INET,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        `,

        // ============================================================
        // جدول الإشعارات
        // ============================================================
        `
        CREATE TABLE IF NOT EXISTS notifications (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            type VARCHAR(50) NOT NULL,
            title VARCHAR(255) NOT NULL,
            message TEXT NOT NULL,
            data JSONB,
            is_read BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        `,

        // ============================================================
        // الفهارس لتحسين الأداء
        // ============================================================
        `CREATE INDEX IF NOT EXISTS idx_users_telegram_id ON users(telegram_id);`,
        `CREATE INDEX IF NOT EXISTS idx_users_username ON users(username);`,
        `CREATE INDEX IF NOT EXISTS idx_users_last_active ON users(last_active);`,
        `CREATE INDEX IF NOT EXISTS idx_users_is_admin ON users(is_admin);`,
        `CREATE INDEX IF NOT EXISTS idx_users_is_banned ON users(is_banned);`,
        `CREATE INDEX IF NOT EXISTS idx_sessions_user_id ON sessions(user_id);`,
        `CREATE INDEX IF NOT EXISTS idx_sessions_token ON sessions(token);`,
        `CREATE INDEX IF NOT EXISTS idx_sessions_refresh_token ON sessions(refresh_token);`,
        `CREATE INDEX IF NOT EXISTS idx_sessions_socket_id ON sessions(socket_id);`,
        `CREATE INDEX IF NOT EXISTS idx_rooms_status ON rooms(status);`,
        `CREATE INDEX IF NOT EXISTS idx_rooms_type ON rooms(type);`,
        `CREATE INDEX IF NOT EXISTS idx_rooms_map_id ON rooms(map_id);`,
        `CREATE INDEX IF NOT EXISTS idx_room_players_room_id ON room_players(room_id);`,
        `CREATE INDEX IF NOT EXISTS idx_room_players_user_id ON room_players(user_id);`,
        `CREATE INDEX IF NOT EXISTS idx_room_players_is_alive ON room_players(is_alive);`,
        `CREATE INDEX IF NOT EXISTS idx_transactions_user_id ON transactions(user_id);`,
        `CREATE INDEX IF NOT EXISTS idx_transactions_created_at ON transactions(created_at);`,
        `CREATE INDEX IF NOT EXISTS idx_transactions_type ON transactions(type);`,
        `CREATE INDEX IF NOT EXISTS idx_audit_logs_user_id ON audit_logs(user_id);`,
        `CREATE INDEX IF NOT EXISTS idx_audit_logs_created_at ON audit_logs(created_at);`,
        `CREATE INDEX IF NOT EXISTS idx_audit_logs_action ON audit_logs(action);`,
        `CREATE INDEX IF NOT EXISTS idx_bans_user_id ON bans(user_id);`,
        `CREATE INDEX IF NOT EXISTS idx_bans_expires_at ON bans(expires_at);`,
        `CREATE INDEX IF NOT EXISTS idx_notifications_user_id ON notifications(user_id);`,
        `CREATE INDEX IF NOT EXISTS idx_notifications_is_read ON notifications(is_read);`,

        // ============================================================
        // الدوال والإجراءات المخزنة
        // ============================================================
        `
        CREATE OR REPLACE FUNCTION update_updated_at_column()
        RETURNS TRIGGER AS $$
        BEGIN
            NEW.updated_at = CURRENT_TIMESTAMP;
            RETURN NEW;
        END;
        $$ language 'plpgsql';
        `,

        // Triggers للتحديث التلقائي
        `
        DROP TRIGGER IF EXISTS update_users_updated_at ON users;
        CREATE TRIGGER update_users_updated_at BEFORE UPDATE ON users
            FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
        `,
        `
        DROP TRIGGER IF EXISTS update_rooms_updated_at ON rooms;
        CREATE TRIGGER update_rooms_updated_at BEFORE UPDATE ON rooms
            FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
        `,
        `
        DROP TRIGGER IF EXISTS update_daily_stats_updated_at ON daily_stats;
        CREATE TRIGGER update_daily_stats_updated_at BEFORE UPDATE ON daily_stats
            FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
        `,

        // ============================================================
        // دالة لتحديث إحصائيات المستخدم المتقدمة
        // ============================================================
        `
        CREATE OR REPLACE FUNCTION update_user_stats(
            p_user_id UUID,
            p_kills INTEGER DEFAULT 0,
            p_deaths INTEGER DEFAULT 0,
            p_assists INTEGER DEFAULT 0,
            p_damage INTEGER DEFAULT 0,
            p_shots_fired INTEGER DEFAULT 0,
            p_shots_hit INTEGER DEFAULT 0,
            p_won BOOLEAN DEFAULT FALSE,
            p_playtime INTEGER DEFAULT 0
        ) RETURNS VOID AS $$
        BEGIN
            UPDATE users SET
                total_kills = total_kills + p_kills,
                total_deaths = total_deaths + p_deaths,
                total_damage_dealt = total_damage_dealt + p_damage,
                games_played = games_played + 1,
                wins = wins + CASE WHEN p_won THEN 1 ELSE 0 END,
                losses = losses + CASE WHEN p_won THEN 0 ELSE 1 END,
                current_streak = CASE
                    WHEN p_won THEN current_streak + 1
                    ELSE 0
                END,
                longest_streak = GREATEST(longest_streak, current_streak + CASE WHEN p_won THEN 1 ELSE 0 END),
                total_playtime = total_playtime + p_playtime,
                accuracy = CASE
                    WHEN (shots_fired + p_shots_fired) > 0 
                    THEN ((shots_hit + p_shots_hit)::FLOAT / (shots_fired + p_shots_fired)::FLOAT) * 100
                    ELSE accuracy
                END
            WHERE id = p_user_id;
        END;
        $$ LANGUAGE plpgsql;
        `,

        // ============================================================
        // دالة لتسجيل المعاملة مع التحقق
        // ============================================================
        `
        CREATE OR REPLACE FUNCTION log_transaction(
            p_user_id UUID,
            p_type VARCHAR(30),
            p_amount INTEGER,
            p_description TEXT DEFAULT NULL,
            p_room_id VARCHAR(50) DEFAULT NULL,
            p_admin_id UUID DEFAULT NULL,
            p_metadata JSONB DEFAULT NULL
        ) RETURNS UUID AS $$
        DECLARE
            v_balance INTEGER;
            v_transaction_id UUID;
            v_new_balance INTEGER;
        BEGIN
            SELECT balance INTO v_balance FROM users WHERE id = p_user_id FOR UPDATE;
            IF v_balance IS NULL THEN
                RAISE EXCEPTION 'User not found';
            END IF;

            -- التحقق من الرصيد للسحب
            IF p_type IN ('withdraw', 'game_entry', 'admin_remove') AND v_balance < p_amount THEN
                RAISE EXCEPTION 'Insufficient balance: % < %', v_balance, p_amount;
            END IF;

            -- حساب الرصيد الجديد
            v_new_balance := CASE
                WHEN p_type IN ('deposit', 'game_reward', 'admin_add', 'refund') THEN v_balance + p_amount
                WHEN p_type IN ('withdraw', 'game_entry', 'admin_remove') THEN v_balance - p_amount
                ELSE v_balance
            END;

            INSERT INTO transactions (
                user_id, type, amount, balance_after, description, room_id, admin_id, metadata
            ) VALUES (
                p_user_id, p_type, p_amount, v_new_balance, p_description, p_room_id, p_admin_id, p_metadata
            ) RETURNING id INTO v_transaction_id;

            UPDATE users SET balance = v_new_balance WHERE id = p_user_id;

            RETURN v_transaction_id;
        END;
        $$ LANGUAGE plpgsql;
        `,

        // ============================================================
        // دالة للحصول على إحصائيات اللاعب
        // ============================================================
        `
        CREATE OR REPLACE FUNCTION get_player_stats(p_user_id UUID)
        RETURNS TABLE (
            games_played INTEGER,
            wins INTEGER,
            losses INTEGER,
            win_rate FLOAT,
            total_kills INTEGER,
            total_deaths INTEGER,
            kd_ratio FLOAT,
            total_damage_dealt INTEGER,
            total_damage_taken INTEGER,
            accuracy FLOAT,
            longest_streak INTEGER,
            current_streak INTEGER
        ) AS $$
        BEGIN
            RETURN QUERY
            SELECT 
                u.games_played,
                u.wins,
                u.losses,
                CASE WHEN u.games_played > 0 
                    THEN (u.wins::FLOAT / u.games_played::FLOAT) * 100 
                    ELSE 0 
                END AS win_rate,
                u.total_kills,
                u.total_deaths,
                CASE WHEN u.total_deaths > 0 
                    THEN u.total_kills::FLOAT / u.total_deaths::FLOAT 
                    ELSE u.total_kills::FLOAT 
                END AS kd_ratio,
                u.total_damage_dealt,
                u.total_damage_taken,
                u.accuracy,
                u.longest_streak,
                u.current_streak
            FROM users u
            WHERE u.id = p_user_id;
        END;
        $$ LANGUAGE plpgsql;
        `,
    ];

    for (const query of queries) {
        try {
            await pool.query(query);
        } catch (error) {
            logger.warn(`⚠️ Query warning: ${error.message}`);
        }
    }

    // ============================================================
    // إنشاء المشرف الافتراضي
    // ============================================================
    const adminTelegramId = parseInt(process.env.ADMIN_TELEGRAM_ID) || 999999999;
    await pool.query(
        `INSERT INTO users (telegram_id, username, balance, is_admin, is_verified)
         VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (telegram_id) DO UPDATE SET is_admin = TRUE, is_verified = TRUE`,
        [adminTelegramId, 'Admin', 99999, true, true]
    );

    logger.info('✅ Database schema initialized successfully!');
    return true;
}

// ================================================================
// 🚀 تهيئة Express مع كل وسائل الأمان المتقدمة
// ================================================================
const app = express();

// ================================================================
// 🛡️ الأمان المتقدم
// ================================================================
app.use(helmet({
    contentSecurityPolicy: {
        directives: {
            defaultSrc: ["'self'"],
            scriptSrc: ["'self'", "'unsafe-inline'", "'unsafe-eval'", 
                "https://cdn.socket.io", "https://unpkg.com", "https://cdnjs.cloudflare.com", 
                "https://www.gstatic.com", "https://cdn.jsdelivr.net"],
            styleSrc: ["'self'", "'unsafe-inline'", "https://fonts.googleapis.com", "https://cdnjs.cloudflare.com"],
            fontSrc: ["'self'", "https://fonts.gstatic.com", "https://cdnjs.cloudflare.com"],
            imgSrc: ["'self'", "data:", "https:", "blob:"],
            connectSrc: ["'self'", "wss:", "https:", "ws:", "blob:"],
            frameSrc: ["'none'"],
            objectSrc: ["'none'"],
            mediaSrc: ["'self'", "https:", "data:"],
            workerSrc: ["'self'", "blob:"],
            upgradeInsecureRequests: [],
        },
    },
    crossOriginEmbedderPolicy: false,
    crossOriginOpenerPolicy: { policy: "same-origin-allow-popups" },
    crossOriginResourcePolicy: { policy: "cross-origin" },
}));

// ================================================================
// 📦 الضغط والتخزين المؤقت المتقدم
// ================================================================
app.use(compression({
    level: 9,
    threshold: 512,
    filter: (req, res) => {
        if (req.headers['x-no-compression']) return false;
        return compression.filter(req, res);
    }
}));

// ================================================================
// 🌐 CORS المتقدم
// ================================================================
const corsOptions = {
    origin: process.env.CORS_ORIGIN ? process.env.CORS_ORIGIN.split(',') : '*',
    methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS', 'PATCH'],
    allowedHeaders: ['Content-Type', 'Authorization', 'X-Requested-With', 'Accept', 'Origin', 'X-CSRF-Token'],
    exposedHeaders: ['X-Total-Count', 'X-Page', 'X-Page-Size', 'X-RateLimit-Limit', 'X-RateLimit-Remaining'],
    credentials: true,
    maxAge: 86400,
    preflightContinue: false,
    optionsSuccessStatus: 204,
};
app.use(cors(corsOptions));

// ================================================================
// 📊 معدل الطلبات (Rate Limiting) المتقدم
// ================================================================
const globalLimiter = rateLimit({
    windowMs: 15 * 60 * 1000,
    max: 1000,
    message: { error: 'Too many requests, please try again later.' },
    standardHeaders: true,
    legacyHeaders: false,
    keyGenerator: (req) => req.ip || req.headers['x-forwarded-for'] || 'unknown',
    skip: (req) => req.path === '/health',
});
app.use('/api/', globalLimiter);

const authLimiter = rateLimit({
    windowMs: 60 * 60 * 1000,
    max: 50,
    message: { error: 'Too many authentication attempts, please try again later.' },
    skipSuccessfulRequests: true,
    keyGenerator: (req) => req.ip || req.headers['x-forwarded-for'] || 'unknown',
});
app.use('/api/auth/', authLimiter);

const gameLimiter = rateLimit({
    windowMs: 60 * 1000,
    max: 100,
    message: { error: 'Too many game requests, slow down.' },
    keyGenerator: (req) => req.ip || req.headers['x-forwarded-for'] || 'unknown',
});
app.use('/api/game/', gameLimiter);

// ================================================================
// 🐌 تباطؤ الطلبات (Slow Down)
// ================================================================
const speedLimiter = slowDown({
    windowMs: 15 * 60 * 1000,
    delayAfter: 100,
    delayMs: (hits) => hits * 50,
    keyGenerator: (req) => req.ip || req.headers['x-forwarded-for'] || 'unknown',
});
app.use(speedLimiter);

// ================================================================
// 📦 معالجة JSON والـ Body مع التحقق
// ================================================================
app.use(express.json({ 
    limit: '100mb',
    verify: (req, res, buf) => {
        try {
            JSON.parse(buf);
        } catch (e) {
            res.status(400).json({ error: 'Invalid JSON' });
            throw new Error('Invalid JSON');
        }
    }
}));
app.use(express.urlencoded({ extended: true, limit: '100mb' }));

// ================================================================
// 📁 تقديم الملفات الثابتة مع التخزين المؤقت المتقدم
// ================================================================
app.use(express.static('public', {
    maxAge: process.env.NODE_ENV === 'production' ? '1y' : 0,
    etag: true,
    lastModified: true,
    setHeaders: (res, path) => {
        if (path.endsWith('.html')) {
            res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
        } else if (path.match(/\.(js|css|png|jpg|jpeg|gif|svg|woff|woff2|ttf|eot)$/)) {
            res.setHeader('Cache-Control', 'public, max-age=31536000, immutable');
        }
    }
}));

// ================================================================
// 📝 سجل الطلبات مع تتبع الوقت
// ================================================================
app.use((req, res, next) => {
    const start = Date.now();
    res.on('finish', () => {
        const duration = Date.now() - start;
        logger.info(`${req.method} ${req.path} - ${res.statusCode} - ${duration}ms - ${req.ip}`);
    });
    next();
});

// ================================================================
// 🏠 نظام الغرف المتقدم مع 4 أنواع و 10 غرف لكل نوع
// ================================================================
const ROOM_TYPES = [
    { name: 'غرفة المبتدئين', maxSeats: 2, seatPrice: 1, prefix: 'beginner', map: 'desert', mapName: 'صحراء' },
    { name: 'غرفة المتقدمين', maxSeats: 4, seatPrice: 5, prefix: 'advanced', map: 'urban', mapName: 'مدينة' },
    { name: 'غرفة المحترفين', maxSeats: 6, seatPrice: 10, prefix: 'pro', map: 'arena', mapName: 'ساحة' },
    { name: 'غرفة الأساطير', maxSeats: 8, seatPrice: 25, prefix: 'legend', map: 'fortress', mapName: 'قلعة' },
];

const ROOMS_PER_TYPE = parseInt(process.env.ROOMS_PER_TYPE) || 10;

// ================================================================
// 📦 التخزين المؤقت في الذاكرة
// ================================================================
const rooms = new Map();
const players = new Map();
const roomTimers = new Map();
const roomIntervals = new Map();
const joinLocks = new Map();
const gameStates = new Map();
const pendingActions = new Map();
const rateLimits = new Map();

// ================================================================
// 🏗️ تهيئة الغرف
// ================================================================
async function initializeRooms() {
    try {
        const result = await pool.query('SELECT COUNT(*) FROM rooms');

        if (parseInt(result.rows[0].count) === 0) {
            logger.info('🏠 Creating initial rooms...');

            for (const type of ROOM_TYPES) {
                for (let i = 1; i <= ROOMS_PER_TYPE; i++) {
                    const roomId = `${type.prefix}_room_${String(i).padStart(2, '0')}`;
                    await pool.query(
                        `INSERT INTO rooms (id, name, type, max_seats, seat_price, status, players_count, map_id, map_name, game_mode)
                         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
                        [
                            roomId,
                            `${type.name} ${i}`,
                            type.name,
                            type.maxSeats,
                            type.seatPrice,
                            'waiting',
                            0,
                            type.map,
                            type.mapName,
                            'team_deathmatch'
                        ]
                    );

                    rooms.set(roomId, {
                        id: roomId,
                        name: `${type.name} ${i}`,
                        type: type.name,
                        maxSeats: type.maxSeats,
                        seatPrice: type.seatPrice,
                        status: 'waiting',
                        players: [],
                        playersCount: 0,
                        mapId: type.map,
                        mapName: type.mapName,
                        gameMode: 'team_deathmatch',
                        startTime: null,
                        gameInterval: null,
                        duration: 300,
                        maxScore: 50,
                    });
                }
            }
            logger.info(`✅ Created ${ROOM_TYPES.length * ROOMS_PER_TYPE} rooms`);
        } else {
            const allRooms = await pool.query('SELECT * FROM rooms ORDER BY id');
            for (const row of allRooms.rows) {
                rooms.set(row.id, {
                    ...row,
                    players: [],
                    gameInterval: null,
                    startTime: null,
                });
            }
            logger.info(`✅ Loaded ${rooms.size} rooms from database`);
        }
    } catch (error) {
        logger.error('❌ Error initializing rooms:', error);
        throw error;
    }
}

// ================================================================
// 📡 خادم Socket.io مع إعدادات متقدمة واحترافية
// ================================================================
const server = http.createServer(app);
const io = socketIo(server, {
    cors: {
        origin: process.env.CORS_ORIGIN ? process.env.CORS_ORIGIN.split(',') : '*',
        methods: ["GET", "POST", "OPTIONS"],
        credentials: true,
    },
    transports: ['websocket', 'polling'],
    pingTimeout: 90000,
    pingInterval: 25000,
    allowEIO3: true,
    maxHttpBufferSize: 1e8,
    perMessageDeflate: {
        threshold: 1024,
        zlib: { level: 9, memLevel: 9 }
    }
});

// ================================================================
// 🔐 دوال المصادقة والتوكن المتقدمة
// ================================================================
const JWT_SECRET = process.env.JWT_SECRET || crypto.randomBytes(64).toString('hex');
const JWT_REFRESH_SECRET = process.env.JWT_REFRESH_SECRET || crypto.randomBytes(64).toString('hex');

function generateTokens(userId) {
    const token = jwt.sign(
        { userId, type: 'access', timestamp: Date.now() },
        JWT_SECRET,
        { expiresIn: '7d', algorithm: 'HS256' }
    );
    const refreshToken = jwt.sign(
        { userId, type: 'refresh', timestamp: Date.now() },
        JWT_REFRESH_SECRET,
        { expiresIn: '30d', algorithm: 'HS256' }
    );
    return { token, refreshToken };
}

function verifyToken(token, isRefresh = false) {
    try {
        return jwt.verify(token, isRefresh ? JWT_REFRESH_SECRET : JWT_SECRET, { algorithms: ['HS256'] });
    } catch (error) {
        if (error.name === 'TokenExpiredError') {
            return { expired: true };
        }
        return null;
    }
}

// ================================================================
// 🔐 دوال مساعدة متقدمة
// ================================================================
async function getUserByTelegramId(telegramId) {
    const result = await pool.query('SELECT * FROM users WHERE telegram_id = $1', [telegramId]);
    return result.rows[0] || null;
}

async function getUserById(userId) {
    const result = await pool.query('SELECT * FROM users WHERE id = $1', [userId]);
    return result.rows[0] || null;
}

async function createUser(telegramId, username, ip) {
    const displayName = username || `User_${telegramId}`;
    const result = await pool.query(
        `INSERT INTO users (telegram_id, telegram_username, username, balance, last_ip, created_at)
         VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP)
         RETURNING *`,
        [telegramId, username || '', displayName, 100, ip || null]
    );
    return result.rows[0];
}

async function createSession(userId, socketId, token, refreshToken, ip, userAgent, deviceId) {
    const expiresAt = new Date();
    expiresAt.setDate(expiresAt.getDate() + 7);

    const refreshExpiresAt = new Date();
    refreshExpiresAt.setDate(refreshExpiresAt.getDate() + 30);

    await pool.query(
        `INSERT INTO sessions (user_id, socket_id, token, refresh_token, ip_address, user_agent, device_id, expires_at, refresh_expires_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
        [userId, socketId, token, refreshToken, ip, userAgent, deviceId || null, expiresAt, refreshExpiresAt]
    );
}

async function updateSessionSocket(token, socketId) {
    await pool.query(
        'UPDATE sessions SET socket_id = $1, last_activity = CURRENT_TIMESTAMP WHERE token = $2',
        [socketId, token]
    );
}

async function getSessionByToken(token) {
    const result = await pool.query(
        'SELECT * FROM sessions WHERE token = $1 AND expires_at > CURRENT_TIMESTAMP',
        [token]
    );
    return result.rows[0] || null;
}

async function getSocketByUserId(userId) {
    const result = await pool.query(
        'SELECT socket_id FROM sessions WHERE user_id = $1 AND socket_id IS NOT NULL ORDER BY created_at DESC LIMIT 1',
        [userId]
    );
    return result.rows[0]?.socket_id || null;
}

async function logAudit(userId, action, details, ip, userAgent, severity = 'info') {
    await pool.query(
        `INSERT INTO audit_logs (user_id, action, details, ip_address, user_agent, severity)
         VALUES ($1, $2, $3, $4, $5, $6)`,
        [userId, action, details, ip, userAgent, severity]
    );
}

async function createNotification(userId, type, title, message, data = null) {
    await pool.query(
        `INSERT INTO notifications (user_id, type, title, message, data)
         VALUES ($1, $2, $3, $4, $5)`,
        [userId, type, title, message, data]
    );
}

// ================================================================
// 🎯 نظام الطوابير المتين (Anti-Cheat & Anti-Exploit)
// ================================================================
class ActionQueue {
    constructor() {
        this.queues = new Map();
        this.processing = new Map();
        this.locks = new Map();
    }

    async enqueue(actionId, userId, action, priority = 0) {
        const key = `${userId}_${actionId}`;
        
        if (this.locks.has(key)) {
            throw new Error('Action already in progress');
        }

        if (!this.queues.has(key)) {
            this.queues.set(key, []);
        }
        const queue = this.queues.get(key);
        queue.push({ action, priority, timestamp: Date.now() });

        await this.processQueue(key, userId);
        return true;
    }

    async processQueue(key, userId) {
        if (this.processing.has(key)) return;
        this.processing.set(key, true);
        this.locks.set(key, true);

        try {
            const queue = this.queues.get(key) || [];
            while (queue.length > 0) {
                const item = queue.shift();
                try {
                    await item.action();
                } catch (error) {
                    logger.error(`❌ Queue action failed for ${userId}:`, error);
                    if (item.retryCount < 1) {
                        item.retryCount = (item.retryCount || 0) + 1;
                        queue.unshift(item);
                        await new Promise(resolve => setTimeout(resolve, 500));
                    }
                }
            }
        } finally {
            this.processing.delete(key);
            this.locks.delete(key);
            this.queues.delete(key);
        }
    }

    isLocked(userId, actionId) {
        return this.locks.has(`${userId}_${actionId}`);
    }

    clear() {
        this.queues.clear();
        this.processing.clear();
        this.locks.clear();
    }
}

const actionQueue = new ActionQueue();

// ================================================================
// 📡 معالجات Socket.io المتقدمة
// ================================================================
io.use(async (socket, next) => {
    const token = socket.handshake.auth.token;
    if (token) {
        try {
            const decoded = verifyToken(token);
            if (decoded && !decoded.expired) {
                const session = await getSessionByToken(token);
                if (session) {
                    socket.userId = session.user_id;
                    socket.token = token;
                    socket.deviceId = socket.handshake.auth.deviceId || null;
                    return next();
                }
            }
        } catch (error) {
            logger.error('❌ Socket auth error:', error);
        }
    }
    
    next();
});

io.on('connection', (socket) => {
    const ip = socket.handshake.address;
    const userAgent = socket.handshake.headers['user-agent'] || 'Unknown';
    const deviceId = socket.handshake.auth.deviceId || null;

    logger.info(`🔌 New connection: ${socket.id} (${ip})`);

    // ============================================================
    // 🔐 المصادقة عبر تيليغرام (مع نظام طوابير)
    // ============================================================
    socket.on('auth', async (data) => {
        try {
            const { telegramId, username, token } = data;

            // محاولة إعادة الاتصال عبر التوكن
            if (token) {
                const decoded = verifyToken(token);
                if (decoded && !decoded.expired) {
                    const session = await getSessionByToken(token);
                    if (session) {
                        await updateSessionSocket(token, socket.id);

                        const user = await getUserById(session.user_id);
                        if (user) {
                            const playerData = {
                                userId: user.id,
                                telegramId: user.telegram_id,
                                username: user.username,
                                balance: user.balance,
                                isAdmin: user.is_admin || false,
                                isBanned: user.is_banned || false,
                                roomId: null,
                                token: token,
                                lastActivity: Date.now(),
                                ip: ip,
                                deviceId: deviceId,
                            };
                            players.set(socket.id, playerData);

                            socket.emit('auth_success', {
                                userId: user.id,
                                telegramId: user.telegram_id,
                                username: user.username,
                                balance: user.balance,
                                isAdmin: user.is_admin || false,
                                isBanned: user.is_banned || false,
                                gamesPlayed: user.games_played || 0,
                                wins: user.wins || 0,
                                totalKills: user.total_kills || 0,
                                token: token,
                                message: '🔄 تم إعادة الاتصال بنجاح!',
                            });

                            await logAudit(
                                user.id,
                                'reconnect',
                                { socketId: socket.id, ip, deviceId },
                                ip,
                                userAgent
                            );

                            logger.info(`🔄 User reconnected: ${user.username}`);
                            return;
                        }
                    }
                }
            }

            // مصادقة جديدة
            if (!telegramId) {
                socket.emit('auth_error', { message: 'معرف تيليغرام مطلوب' });
                return;
            }

            // التحقق من الحظر
            const banCheck = await pool.query(
                `SELECT b.* FROM bans b
                 JOIN users u ON u.id = b.user_id
                 WHERE u.telegram_id = $1 AND (b.expires_at IS NULL OR b.expires_at > CURRENT_TIMESTAMP)`,
                [telegramId]
            );

            if (banCheck.rows.length > 0) {
                const ban = banCheck.rows[0];
                socket.emit('auth_error', {
                    message: `🚫 تم حظر حسابك.\nالسبب: ${ban.reason || 'غير محدد'}\n${ban.expires_at ? `ينتهي: ${new Date(ban.expires_at).toLocaleDateString('ar')}` : 'دائم'}`,
                    banned: true,
                    reason: ban.reason,
                    expiresAt: ban.expires_at,
                });
                return;
            }

            // معالجة المصادقة عبر الطابور
            await actionQueue.enqueue('auth', telegramId, async () => {
                let user = await getUserByTelegramId(telegramId);
                let isNewUser = false;

                if (!user) {
                    user = await createUser(telegramId, username, ip);
                    isNewUser = true;
                    logger.info(`👤 New user registered: ${user.username} (${telegramId})`);
                } else {
                    await pool.query(
                        'UPDATE users SET last_active = CURRENT_TIMESTAMP, last_ip = $1 WHERE telegram_id = $2',
                        [ip, telegramId]
                    );
                }

                const { token: newToken, refreshToken } = generateTokens(user.id);

                await createSession(
                    user.id,
                    socket.id,
                    newToken,
                    refreshToken,
                    ip,
                    userAgent,
                    deviceId
                );

                const playerData = {
                    userId: user.id,
                    telegramId: user.telegram_id,
                    username: user.username,
                    balance: user.balance,
                    isAdmin: user.is_admin || false,
                    isBanned: user.is_banned || false,
                    roomId: null,
                    token: newToken,
                    lastActivity: Date.now(),
                    ip: ip,
                    deviceId: deviceId,
                };
                players.set(socket.id, playerData);

                await logAudit(
                    user.id,
                    isNewUser ? 'register' : 'login',
                    { socketId: socket.id, ip, deviceId, method: 'telegram' },
                    ip,
                    userAgent
                );

                socket.emit('auth_success', {
                    userId: user.id,
                    telegramId: user.telegram_id,
                    username: user.username,
                    balance: user.balance,
                    isAdmin: user.is_admin || false,
                    isBanned: user.is_banned || false,
                    gamesPlayed: user.games_played || 0,
                    wins: user.wins || 0,
                    totalKills: user.total_kills || 0,
                    token: newToken,
                    refreshToken: refreshToken,
                    isNewUser: isNewUser,
                    message: isNewUser ? '✅ تم إنشاء الحساب بنجاح!' : '✅ تم تسجيل الدخول بنجاح!',
                });

                if (isNewUser) {
                    await createNotification(
                        user.id,
                        'welcome',
                        '🎉 مرحباً بك في Battle Tanks!',
                        'نتمنى لك معارك ممتعة وننتظر انتصاراتك! 🏆'
                    );
                }

                logger.info(`✅ Authenticated: ${user.username} (${telegramId})`);
            });

        } catch (error) {
            logger.error('❌ Auth error:', error);
            socket.emit('auth_error', { message: 'فشلت المصادقة: ' + error.message });
        }
    });

    // ============================================================
    // 🔄 تحديث التوكن
    // ============================================================
    socket.on('refresh_token', async (data) => {
        try {
            const { refreshToken } = data;
            if (!refreshToken) {
                socket.emit('refresh_error', { message: 'Refresh token required' });
                return;
            }

            const decoded = verifyToken(refreshToken, true);
            if (!decoded || decoded.expired) {
                socket.emit('refresh_error', { message: 'Invalid or expired refresh token' });
                return;
            }

            const session = await pool.query(
                'SELECT * FROM sessions WHERE refresh_token = $1 AND refresh_expires_at > CURRENT_TIMESTAMP',
                [refreshToken]
            );

            if (session.rows.length === 0) {
                socket.emit('refresh_error', { message: 'Refresh token expired' });
                return;
            }

            const { token: newToken, refreshToken: newRefreshToken } = generateTokens(decoded.userId);

            await pool.query(
                `UPDATE sessions SET token = $1, refresh_token = $2, 
                 expires_at = CURRENT_TIMESTAMP + INTERVAL '7 days', 
                 refresh_expires_at = CURRENT_TIMESTAMP + INTERVAL '30 days' 
                 WHERE refresh_token = $3`,
                [newToken, newRefreshToken, refreshToken]
            );

            const player = players.get(socket.id);
            if (player) {
                player.token = newToken;
            }

            socket.emit('refresh_success', {
                token: newToken,
                refreshToken: newRefreshToken,
            });

        } catch (error) {
            logger.error('❌ Refresh token error:', error);
            socket.emit('refresh_error', { message: 'Failed to refresh token' });
        }
    });

    // ============================================================
    // 🏠 اللوبي
    // ============================================================
    socket.on('join_lobby', async () => {
        const player = players.get(socket.id);
        if (!player) {
            socket.emit('error', { message: 'غير مصدق' });
            return;
        }

        try {
            const result = await pool.query(
                'SELECT balance, games_played, wins, total_kills, total_deaths, accuracy FROM users WHERE id = $1',
                [player.userId]
            );

            if (result.rows.length > 0) {
                player.balance = result.rows[0].balance;
                player.gamesPlayed = result.rows[0].games_played || 0;
                player.wins = result.rows[0].wins || 0;
                player.totalKills = result.rows[0].total_kills || 0;
                player.totalDeaths = result.rows[0].total_deaths || 0;
                player.accuracy = result.rows[0].accuracy || 0;
            }

            socket.emit('lobby_joined', {
                balance: player.balance,
                userId: player.userId,
                username: player.username,
                isAdmin: player.isAdmin,
                gamesPlayed: player.gamesPlayed || 0,
                wins: player.wins || 0,
                totalKills: player.totalKills || 0,
                totalDeaths: player.totalDeaths || 0,
                accuracy: player.accuracy || 0,
                isBanned: player.isBanned || false,
            });

            await sendRoomsList(socket);

            const notifications = await pool.query(
                'SELECT * FROM notifications WHERE user_id = $1 AND is_read = FALSE ORDER BY created_at DESC LIMIT 10',
                [player.userId]
            );
            if (notifications.rows.length > 0) {
                socket.emit('notifications', { notifications: notifications.rows });
            }

        } catch (error) {
            logger.error('❌ Lobby error:', error);
            socket.emit('error', { message: 'فشل تحميل اللوبي' });
        }
    });

    // ============================================================
    // 📋 قائمة الغرف
    // ============================================================
    let lastRoomListRequest = 0;

    socket.on('list_rooms', async () => {
        const now = Date.now();
        if (now - lastRoomListRequest < 2000) return;
        lastRoomListRequest = now;
        await sendRoomsList(socket);
    });

    async function sendRoomsList(targetSocket) {
        try {
            const result = await pool.query(
                `SELECT id, name, type, max_seats, seat_price, status, players_count, map_id, map_name
                 FROM rooms
                 WHERE status = 'waiting'
                 ORDER BY 
                     CASE type
                         WHEN 'غرفة المبتدئين' THEN 1
                         WHEN 'غرفة المتقدمين' THEN 2
                         WHEN 'غرفة المحترفين' THEN 3
                         WHEN 'غرفة الأساطير' THEN 4
                         ELSE 5
                     END,
                     created_at`
            );

            const roomsList = result.rows.map(room => ({
                id: room.id,
                name: room.name,
                type: room.type,
                maxSeats: room.max_seats,
                seatPrice: room.seat_price,
                players: room.players_count || 0,
                status: room.status,
                needed: room.max_seats - (room.players_count || 0),
                mapId: room.map_id || 'default',
                mapName: room.map_name || 'صحراء',
                isFull: room.players_count >= room.max_seats,
            }));

            targetSocket.emit('rooms_list', { 
                rooms: roomsList,
                total: roomsList.length,
                timestamp: Date.now(),
            });

        } catch (error) {
            logger.error('❌ Error sending rooms list:', error);
            targetSocket.emit('error', { message: 'فشل تحميل قائمة الغرف' });
        }
    }

    async function sendRoomsListToAll() {
        try {
            const result = await pool.query(
                `SELECT id, name, type, max_seats, seat_price, status, players_count, map_id, map_name
                 FROM rooms
                 WHERE status = 'waiting'
                 ORDER BY 
                     CASE type
                         WHEN 'غرفة المبتدئين' THEN 1
                         WHEN 'غرفة المتقدمين' THEN 2
                         WHEN 'غرفة المحترفين' THEN 3
                         WHEN 'غرفة الأساطير' THEN 4
                         ELSE 5
                     END,
                     created_at`
            );

            const roomsList = result.rows.map(room => ({
                id: room.id,
                name: room.name,
                type: room.type,
                maxSeats: room.max_seats,
                seatPrice: room.seat_price,
                players: room.players_count || 0,
                status: room.status,
                needed: room.max_seats - (room.players_count || 0),
                mapId: room.map_id || 'default',
                mapName: room.map_name || 'صحراء',
                isFull: room.players_count >= room.max_seats,
            }));

            io.emit('rooms_list', { 
                rooms: roomsList,
                total: roomsList.length,
                timestamp: Date.now(),
            });

        } catch (error) {
            logger.error('❌ Error sending rooms list to all:', error);
        }
    }

    // ============================================================
    // 🎮 الانضمام إلى غرفة (مع أقفال متعددة ونظام طوابير)
    // ============================================================
    socket.on('join_room', async (data) => {
        const player = players.get(socket.id);
        if (!player) {
            socket.emit('error', { message: 'غير مصدق' });
            return;
        }

        const lockKey = `join_${player.userId}`;
        if (joinLocks.has(lockKey)) {
            socket.emit('error', { message: 'جاري معالجة طلبك، انتظر قليلاً...' });
            return;
        }
        joinLocks.set(lockKey, true);
        setTimeout(() => joinLocks.delete(lockKey), 5000);

        try {
            const { roomId } = data;

            await actionQueue.enqueue('join_room', player.userId, async () => {
                const roomResult = await pool.query(
                    'SELECT * FROM rooms WHERE id = $1 FOR UPDATE',
                    [roomId]
                );

                if (roomResult.rows.length === 0) {
                    socket.emit('error', { message: 'الغرفة غير موجودة' });
                    return;
                }

                const room = roomResult.rows[0];

                if (room.status !== 'waiting') {
                    socket.emit('error', { message: 'المعركة جارية، انتظر حتى تنتهي' });
                    return;
                }

                if (room.players_count >= room.max_seats) {
                    socket.emit('error', { message: 'الغرفة ممتلئة' });
                    return;
                }

                const existingPlayer = await pool.query(
                    'SELECT * FROM room_players WHERE room_id = $1 AND user_id = $2 AND left_at IS NULL',
                    [roomId, player.userId]
                );

                if (existingPlayer.rows.length > 0) {
                    socket.emit('error', { message: 'أنت بالفعل في هذه الغرفة' });
                    return;
                }

                const userResult = await pool.query(
                    'SELECT balance FROM users WHERE id = $1 FOR UPDATE',
                    [player.userId]
                );

                const balance = userResult.rows[0]?.balance || 0;
                if (balance < room.seat_price) {
                    socket.emit('error', {
                        message: `⚠️ رصيدك غير كافٍ!\nسعر المقعد: ${room.seat_price}$\nرصيدك الحالي: ${balance}$`
                    });
                    return;
                }

                const newBalance = balance - room.seat_price;
                await pool.query(
                    'UPDATE users SET balance = $1 WHERE id = $2',
                    [newBalance, player.userId]
                );

                await pool.query(
                    `INSERT INTO transactions (user_id, type, amount, balance_after, description, room_id)
                     VALUES ($1, $2, $3, $4, $5, $6)`,
                    [player.userId, 'game_entry', room.seat_price, newBalance, `دخول إلى ${room.name}`, roomId]
                );

                await pool.query(
                    `INSERT INTO room_players (room_id, user_id, socket_id, paid_amount, team, position_x, position_z)
                     VALUES ($1, $2, $3, $4, $5, $6, $7)`,
                    [roomId, player.userId, socket.id, room.seat_price, 1, 0, 0]
                );

                const newCount = room.players_count + 1;
                await pool.query(
                    'UPDATE rooms SET players_count = $1 WHERE id = $2',
                    [newCount, roomId]
                );

                player.roomId = roomId;
                player.balance = newBalance;
                player.team = 1;
                socket.join(roomId);

                const needed = room.max_seats - newCount;
                socket.emit('room_joined', {
                    roomId: roomId,
                    roomName: room.name,
                    roomType: room.type,
                    balance: newBalance,
                    playersCount: newCount,
                    maxSeats: room.max_seats,
                    seatPrice: room.seat_price,
                    needed: needed,
                    mapId: room.map_id || 'default',
                    mapName: room.map_name || 'صحراء',
                    isFull: newCount >= room.max_seats,
                    message: `✅ تم الانضمام إلى ${room.name}\n💰 تم خصم ${room.seat_price}$\n👥 ${newCount}/${room.max_seats}\n⏳ ينتظر ${needed} لاعب(ين)`,
                });

                io.to(roomId).emit('player_joined', {
                    userId: player.userId,
                    username: player.username,
                    playersCount: newCount,
                    maxSeats: room.max_seats,
                    needed: needed,
                    team: 1,
                });

                await sendRoomsListToAll();

                await logAudit(
                    player.userId,
                    'join_room',
                    { roomId, roomName: room.name, price: room.seat_price },
                    ip,
                    userAgent
                );

                logger.info(`👥 ${player.username} joined ${room.name} (${newCount}/${room.max_seats})`);

                if (newCount >= room.max_seats) {
                    await startGame(roomId);
                }

            });

        } catch (error) {
            logger.error('❌ Join room error:', error);
            socket.emit('error', { message: 'فشل الانضمام إلى الغرفة: ' + error.message });
        } finally {
            joinLocks.delete(lockKey);
        }
    });

    // ============================================================
    // 🚪 مغادرة الغرفة (مع إعادة الرصيد)
    // ============================================================
    socket.on('leave_room', async () => {
        const player = players.get(socket.id);
        if (!player || !player.roomId) {
            socket.emit('error', { message: 'أنت لست في أي غرفة' });
            return;
        }

        try {
            const roomId = player.roomId;

            const roomResult = await pool.query(
                'SELECT status, seat_price, name FROM rooms WHERE id = $1',
                [roomId]
            );

            if (roomResult.rows.length === 0) {
                player.roomId = null;
                return;
            }

            const room = roomResult.rows[0];

            if (room.status !== 'waiting') {
                socket.emit('error', { message: 'لا يمكن مغادرة الغرفة أثناء المعركة' });
                return;
            }

            const rpResult = await pool.query(
                `DELETE FROM room_players
                 WHERE room_id = $1 AND user_id = $2
                 RETURNING paid_amount`,
                [roomId, player.userId]
            );

            if (rpResult.rows.length > 0) {
                const paidAmount = rpResult.rows[0].paid_amount || room.seat_price;

                await pool.query(
                    'UPDATE users SET balance = balance + $1 WHERE id = $2',
                    [paidAmount, player.userId]
                );

                const balanceResult = await pool.query(
                    'SELECT balance FROM users WHERE id = $1',
                    [player.userId]
                );
                player.balance = balanceResult.rows[0]?.balance || 100;

                await pool.query(
                    `INSERT INTO transactions (user_id, type, amount, balance_after, description, room_id)
                     VALUES ($1, $2, $3, $4, $5, $6)`,
                    [player.userId, 'refund', paidAmount, player.balance, `مغادرة ${room.name}`, roomId]
                );
            }

            await pool.query(
                'UPDATE rooms SET players_count = players_count - 1 WHERE id = $1',
                [roomId]
            );

            player.roomId = null;
            socket.leave(roomId);

            socket.emit('room_left', {
                roomName: room.name,
                balance: player.balance,
                message: '🚪 تم مغادرة الغرفة بنجاح\n💰 تم إعادة الرصيد',
            });

            io.to(roomId).emit('player_left', {
                userId: player.userId,
                username: player.username,
            });

            await sendRoomsListToAll();
            logger.info(`🚪 ${player.username} left room ${roomId}`);

        } catch (error) {
            logger.error('❌ Leave room error:', error);
            socket.emit('error', { message: 'فشل مغادرة الغرفة' });
        }
    });

    // ============================================================
    // 🎮 بدء اللعبة (مع توزيع متقدم للفرق)
    // ============================================================
    async function startGame(roomId) {
        try {
            logger.info(`🎮 Starting game in room ${roomId}`);

            await pool.query(
                'UPDATE rooms SET status = $1, started_at = CURRENT_TIMESTAMP WHERE id = $2',
                ['active', roomId]
            );

            const playersResult = await pool.query(
                `SELECT rp.*, u.username, u.id as user_id, u.balance
                 FROM room_players rp
                 JOIN users u ON rp.user_id = u.id
                 WHERE rp.room_id = $1 AND rp.left_at IS NULL`,
                [roomId]
            );

            const roomPlayers = playersResult.rows;

            if (roomPlayers.length < 2) {
                logger.warn(`⚠️ Not enough players in room ${roomId}, cancelling game start`);
                await resetRoom(roomId);
                return;
            }

            // توزيع الفرق بشكل متوازن
            const shuffled = [...roomPlayers].sort(() => Math.random() - 0.5);
            const sorted = [...roomPlayers].sort((a, b) => 
                (b.wins || 0) - (a.wins || 0) || (b.games_played || 0) - (a.games_played || 0)
            );

            const team1 = [];
            const team2 = [];
            
            for (let i = 0; i < sorted.length; i++) {
                if (i % 2 === 0) {
                    team1.push(sorted[i]);
                } else {
                    team2.push(sorted[i]);
                }
            }

            const startPositions = [
                { x: -120, z: -80, team: 1 },
                { x: 120, z: 80, team: 2 },
                { x: -120, z: 80, team: 1 },
                { x: 120, z: -80, team: 2 },
                { x: -150, z: 0, team: 1 },
                { x: 150, z: 0, team: 2 },
                { x: -80, z: -120, team: 1 },
                { x: 80, z: 120, team: 2 },
                { x: -180, z: -60, team: 1 },
                { x: 180, z: 60, team: 2 },
                { x: -60, z: -180, team: 1 },
                { x: 60, z: 180, team: 2 },
            ];

            const playerData = [];
            let posIndex = 0;

            for (const player of roomPlayers) {
                const isTeam1 = team1.some(p => p.user_id === player.user_id);
                const team = isTeam1 ? 1 : 2;
                const pos = startPositions[posIndex % startPositions.length];
                posIndex++;

                await pool.query(
                    `UPDATE room_players
                     SET team = $1, position_x = $2, position_z = $3, health = 100, is_alive = TRUE
                     WHERE room_id = $4 AND user_id = $5`,
                    [team, pos.x, pos.z, roomId, player.user_id]
                );

                playerData.push({
                    userId: player.user_id,
                    username: player.username,
                    team: team,
                    position: { x: pos.x, z: pos.z },
                    health: 100,
                    balance: player.balance,
                    kills: 0,
                    deaths: 0,
                });
            }

            const roomInfo = rooms.get(roomId);
            
            for (const player of roomPlayers) {
                const playerInfo = playerData.find(p => p.userId === player.user_id);
                const socketId = player.socket_id;

                if (socketId) {
                    io.to(socketId).emit('game_start', {
                        roomId: roomId,
                        roomName: roomInfo?.name || 'المعركة',
                        mapId: roomInfo?.mapId || 'desert',
                        mapName: roomInfo?.mapName || 'صحراء',
                        gameMode: roomInfo?.gameMode || 'team_deathmatch',
                        players: playerData.map(p => ({
                            userId: p.userId,
                            username: p.username,
                            team: p.team,
                            health: p.health,
                        })),
                        yourTeam: playerInfo?.team || 1,
                        position: playerInfo?.position || { x: 0, z: 0 },
                        health: 100,
                        startTime: Date.now(),
                        duration: roomInfo?.duration || 300,
                        maxScore: roomInfo?.maxScore || 50,
                        team1Players: team1.map(p => p.username),
                        team2Players: team2.map(p => p.username),
                    });
                }
            }

            io.to(roomId).emit('game_started', {
                roomId: roomId,
                playersCount: roomPlayers.length,
                teams: {
                    team1: team1.map(p => p.username),
                    team2: team2.map(p => p.username),
                },
            });

            // بدء تحديثات الحالة
            let gameState = {
                roomId: roomId,
                players: playerData,
                startTime: Date.now(),
                lastUpdate: Date.now(),
                isActive: true,
                team1Score: 0,
                team2Score: 0,
            };

            gameStates.set(roomId, gameState);

            const interval = setInterval(async () => {
                try {
                    const statusResult = await pool.query(
                        'SELECT status FROM rooms WHERE id = $1',
                        [roomId]
                    );

                    if (statusResult.rows.length === 0 || statusResult.rows[0].status !== 'active') {
                        clearInterval(interval);
                        gameStates.delete(roomId);
                        return;
                    }

                    const posResult = await pool.query(
                        `SELECT user_id, position_x, position_z, rotation, health, team, is_alive, kills, deaths
                         FROM room_players
                         WHERE room_id = $1 AND left_at IS NULL`,
                        [roomId]
                    );

                    const playersUpdate = posResult.rows
                        .filter(p => p.is_alive)
                        .map(p => ({
                            userId: p.user_id,
                            position: { x: p.position_x, z: p.position_z },
                            rotation: p.rotation || 0,
                            health: p.health || 100,
                            team: p.team,
                            isAlive: p.is_alive,
                            kills: p.kills || 0,
                            deaths: p.deaths || 0,
                        }));

                    const team1Alive = posResult.rows.filter(p => p.team === 1 && p.is_alive).length;
                    const team2Alive = posResult.rows.filter(p => p.team === 2 && p.is_alive).length;

                    io.to(roomId).emit('game_state_update', {
                        players: playersUpdate,
                        team1Alive: team1Alive,
                        team2Alive: team2Alive,
                        timestamp: Date.now(),
                    });

                } catch (error) {
                    logger.error('❌ Game state update error:', error);
                }
            }, 33);

            roomIntervals.set(roomId, interval);

            const gameDuration = (roomInfo?.duration || 300) * 1000;
            const timer = setTimeout(async () => {
                await endGame(roomId, '⏰ انتهت مدة المعركة!');
            }, gameDuration);

            roomTimers.set(roomId, timer);

            logger.info(`✅ Game started in room ${roomId} with ${roomPlayers.length} players`);

        } catch (error) {
            logger.error('❌ Start game error:', error);
        }
    }

    // ============================================================
    // 🏆 إنهاء اللعبة (مع توزيع المكافآت المتقدمة)
    // ============================================================
    async function endGame(roomId, reason) {
        try {
            logger.info(`🏆 Ending game in room ${roomId}`);

            if (roomTimers.has(roomId)) {
                clearTimeout(roomTimers.get(roomId));
                roomTimers.delete(roomId);
            }

            if (roomIntervals.has(roomId)) {
                clearInterval(roomIntervals.get(roomId));
                roomIntervals.delete(roomId);
            }

            const room = rooms.get(roomId);
            if (room && room.gameInterval) {
                clearInterval(room.gameInterval);
                room.gameInterval = null;
            }

            const playersResult = await pool.query(
                `SELECT rp.*, u.username, u.id as user_id
                 FROM room_players rp
                 JOIN users u ON rp.user_id = u.id
                 WHERE rp.room_id = $1 AND rp.left_at IS NULL`,
                [roomId]
            );

            const alivePlayers = playersResult.rows.filter(p => p.is_alive && p.health > 0);

            let winnerTeam = null;
            let winnerName = 'تعادل';
            let team1Score = 0;
            let team2Score = 0;

            for (const p of playersResult.rows) {
                if (p.team === 1) {
                    team1Score += (p.kills || 0) * 10 + (p.damage_dealt || 0) / 100;
                } else {
                    team2Score += (p.kills || 0) * 10 + (p.damage_dealt || 0) / 100;
                }
            }

            if (team1Score > team2Score) {
                winnerTeam = 1;
                winnerName = 'الفريق الأول 🟢';
            } else if (team2Score > team1Score) {
                winnerTeam = 2;
                winnerName = 'الفريق الثاني 🔴';
            } else {
                const aliveTeam1 = alivePlayers.filter(p => p.team === 1).length;
                const aliveTeam2 = alivePlayers.filter(p => p.team === 2).length;
                if (aliveTeam1 > aliveTeam2) {
                    winnerTeam = 1;
                    winnerName = 'الفريق الأول 🟢 (حسم بالصحة)';
                } else if (aliveTeam2 > aliveTeam1) {
                    winnerTeam = 2;
                    winnerName = 'الفريق الثاني 🔴 (حسم بالصحة)';
                } else {
                    winnerName = 'تعادل تام 🤝';
                }
            }

            const baseReward = 10;
            const killBonus = 2;
            const damageBonus = 1;

            const playerUpdates = [];

            for (const player of playersResult.rows) {
                const isWinner = (player.team === winnerTeam && player.is_alive);
                const kills = player.kills || 0;
                const damage = player.damage_dealt || 0;
                
                let reward = 0;
                if (isWinner) {
                    reward = baseReward + (kills * killBonus) + Math.floor(damage / 200 * damageBonus);
                } else if (kills > 0) {
                    reward = Math.floor(kills * killBonus / 2);
                }

                await pool.query(
                    `SELECT update_user_stats($1, $2, $3, $4, $5, $6, $7, $8)`,
                    [
                        player.user_id,
                        kills,
                        player.deaths || 0,
                        0,
                        damage,
                        player.shots_fired || 0,
                        player.shots_hit || 0,
                        isWinner,
                        Math.floor((Date.now() - (room?.startTime || Date.now())) / 1000)
                    ]
                );

                let newBalance = 0;
                if (reward > 0) {
                    const balanceResult = await pool.query(
                        'UPDATE users SET balance = balance + $1 WHERE id = $2 RETURNING balance',
                        [reward, player.user_id]
                    );
                    newBalance = balanceResult.rows[0]?.balance || 0;

                    await pool.query(
                        `INSERT INTO transactions (user_id, type, amount, balance_after, description, room_id)
                         VALUES ($1, $2, $3, $4, $5, $6)`,
                        [player.user_id, 'game_reward', reward, newBalance, `مكافأة من ${room.name}`, roomId]
                    );
                } else {
                    const balanceResult = await pool.query(
                        'SELECT balance FROM users WHERE id = $1',
                        [player.user_id]
                    );
                    newBalance = balanceResult.rows[0]?.balance || 0;
                }

                playerUpdates.push({
                    userId: player.user_id,
                    socketId: player.socket_id,
                    isWinner: isWinner,
                    reward: reward,
                    newBalance: newBalance,
                    team: player.team,
                    kills: kills,
                    deaths: player.deaths || 0,
                    damage: damage,
                });
            }

            const duration = Math.floor((Date.now() - (room?.startTime || Date.now())) / 1000);

            for (const update of playerUpdates) {
                if (update.socketId) {
                    io.to(update.socketId).emit('game_ended', {
                        message: reason || 'انتهت المعركة!',
                        winner: winnerName,
                        winnerTeam: winnerTeam,
                        reward: update.reward,
                        yourBalance: update.newBalance,
                        yourTeam: update.team === 1 ? 'الفريق الأول 🟢' : 'الفريق الثاني 🔴',
                        duration: duration,
                        isWinner: update.isWinner,
                        kills: update.kills,
                        deaths: update.deaths,
                        damage: update.damage,
                    });
                }
            }

            io.to(roomId).emit('game_finished', {
                roomId: roomId,
                winner: winnerName,
                winnerTeam: winnerTeam,
                duration: duration,
                team1Score: team1Score,
                team2Score: team2Score,
            });

            await pool.query(
                `UPDATE rooms
                 SET status = 'ended', ended_at = CURRENT_TIMESTAMP, winner_team = $1, game_data = $2
                 WHERE id = $3`,
                [winnerTeam, JSON.stringify({ team1Score, team2Score, duration }), roomId]
            );

            logger.info(`🏆 Game ended in room ${roomId}, winner: ${winnerName}`);

            setTimeout(async () => {
                await resetRoom(roomId);
            }, 5000);

            await sendRoomsListToAll();

        } catch (error) {
            logger.error('❌ End game error:', error);
        }
    }

    // ============================================================
    // 🔄 إعادة تعيين الغرفة
    // ============================================================
    async function resetRoom(roomId) {
        try {
            logger.info(`🔄 Resetting room ${roomId}`);

            await pool.query(
                `UPDATE room_players SET left_at = CURRENT_TIMESTAMP WHERE room_id = $1 AND left_at IS NULL`,
                [roomId]
            );

            await pool.query(
                `UPDATE rooms
                 SET status = 'waiting', players_count = 0, started_at = NULL, ended_at = NULL, winner_team = NULL
                 WHERE id = $1`,
                [roomId]
            );

            if (rooms.has(roomId)) {
                const room = rooms.get(roomId);
                room.status = 'waiting';
                room.players = [];
                room.playersCount = 0;
                room.startTime = null;
                room.gameInterval = null;
            }

            gameStates.delete(roomId);

            logger.info(`✅ Room ${roomId} reset successfully`);

            io.to(roomId).emit('room_reset', {
                roomId: roomId,
                message: '✅ الغرفة جاهزة لمعركة جديدة!',
            });

            await sendRoomsListToAll();

        } catch (error) {
            logger.error('❌ Reset room error:', error);
        }
    }

    // ============================================================
    // 🎯 أحداث اللعبة
    // ============================================================
    let lastMoveTime = new Map();

    socket.on('move', async (data) => {
        const player = players.get(socket.id);
        if (!player || !player.roomId) return;

        const now = Date.now();
        const lastMove = lastMoveTime.get(socket.id) || 0;
        if (now - lastMove < 16) return;
        lastMoveTime.set(socket.id, now);

        try {
            const check = await pool.query(
                'SELECT is_alive FROM room_players WHERE room_id = $1 AND user_id = $2',
                [player.roomId, player.userId]
            );

            if (check.rows.length === 0 || !check.rows[0].is_alive) return;

            await pool.query(
                `UPDATE room_players
                 SET position_x = $1, position_z = $2, rotation = $3
                 WHERE room_id = $4 AND user_id = $5`,
                [data.position.x, data.position.z, data.rotation || 0, player.roomId, player.userId]
            );

            socket.to(player.roomId).emit('player_moved', {
                userId: player.userId,
                position: data.position,
                rotation: data.rotation,
                timestamp: now,
            });

        } catch (error) {
            logger.error('❌ Move error:', error);
        }
    });

    let lastShotTime = new Map();

    socket.on('shoot', (data) => {
        const player = players.get(socket.id);
        if (!player || !player.roomId) return;

        const now = Date.now();
        const lastShot = lastShotTime.get(socket.id) || 0;
        if (now - lastShot < 500) return;
        lastShotTime.set(socket.id, now);

        pool.query(
            'UPDATE room_players SET shots_fired = shots_fired + 1 WHERE room_id = $1 AND user_id = $2',
            [player.roomId, player.userId]
        ).catch(err => logger.error('❌ Shot update error:', err));

        socket.to(player.roomId).emit('player_shot', {
            userId: player.userId,
            position: data.position,
            direction: data.direction,
            bulletId: data.bulletId || `${player.userId}_${now}`,
            timestamp: now,
        });
    });

    socket.on('damage', async (data) => {
        const player = players.get(socket.id);
        if (!player || !player.roomId) return;

        try {
            const { targetId, damage } = data;

            if (damage > 100 || damage < 1) {
                logger.warn(`⚠️ Suspicious damage value from ${player.username}: ${damage}`);
                return;
            }

            const targetCheck = await pool.query(
                'SELECT is_alive, team, health FROM room_players WHERE room_id = $1 AND user_id = $2',
                [player.roomId, targetId]
            );

            if (targetCheck.rows.length === 0 || !targetCheck.rows[0].is_alive) return;

            const target = targetCheck.rows[0];
            
            if (target.team === player.team) {
                return;
            }

            const result = await pool.query(
                `UPDATE room_players
                 SET health = GREATEST(0, health - $1), damage_taken = damage_taken + $1
                 WHERE room_id = $2 AND user_id = $3 AND is_alive = TRUE
                 RETURNING health`,
                [damage, player.roomId, targetId]
            );

            if (result.rows.length > 0) {
                const newHealth = result.rows[0].health;

                await pool.query(
                    `UPDATE room_players SET damage_dealt = damage_dealt + $1, shots_hit = shots_hit + 1
                     WHERE room_id = $2 AND user_id = $3`,
                    [damage, player.roomId, player.userId]
                );

                io.to(player.roomId).emit('health_update', {
                    userId: targetId,
                    health: newHealth,
                    damage: damage,
                    attackerId: player.userId,
                });

                if (newHealth <= 0) {
                    await pool.query(
                        `UPDATE room_players SET is_alive = FALSE, deaths = deaths + 1
                         WHERE room_id = $1 AND user_id = $2`,
                        [player.roomId, targetId]
                    );

                    await pool.query(
                        `UPDATE room_players SET kills = kills + 1
                         WHERE room_id = $1 AND user_id = $2`,
                        [player.roomId, player.userId]
                    );

                    io.to(player.roomId).emit('player_eliminated', {
                        userId: targetId,
                        killerId: player.userId,
                        killerName: player.username,
                        position: data.position,
                    });

                    const targetSocket = await getSocketByUserId(targetId);
                    if (targetSocket) {
                        io.to(targetSocket).emit('you_were_eliminated', {
                            message: '💀 لقد تم تدمير دبابتك!',
                            killerId: player.userId,
                            killerName: player.username,
                        });
                    }

                    const aliveResult = await pool.query(
                        `SELECT COUNT(*) FROM room_players
                         WHERE room_id = $1 AND is_alive = TRUE AND left_at IS NULL`,
                        [player.roomId]
                    );

                    if (parseInt(aliveResult.rows[0].count) <= 1) {
                        await endGame(player.roomId, '🏆 انتهت المعركة!');
                    }
                }
            }

        } catch (error) {
            logger.error('❌ Damage error:', error);
        }
    });

    // ============================================================
    // 📊 طلب الإحصائيات
    // ============================================================
    socket.on('get_stats', async () => {
        const player = players.get(socket.id);
        if (!player) return;

        try {
            const result = await pool.query(
                `SELECT * FROM get_player_stats($1)`,
                [player.userId]
            );

            if (result.rows.length > 0) {
                socket.emit('stats_response', {
                    stats: result.rows[0],
                    balance: player.balance,
                });
            }

        } catch (error) {
            logger.error('❌ Stats error:', error);
            socket.emit('error', { message: 'فشل تحميل الإحصائيات' });
        }
    });

    // ============================================================
    // 💰 طلب سجل المعاملات
    // ============================================================
    socket.on('get_transactions', async (data) => {
        const player = players.get(socket.id);
        if (!player) return;

        try {
            const limit = data?.limit || 20;
            const offset = data?.offset || 0;

            const result = await pool.query(
                `SELECT type, amount, balance_after, description, room_id, created_at
                 FROM transactions
                 WHERE user_id = $1
                 ORDER BY created_at DESC
                 LIMIT $2 OFFSET $3`,
                [player.userId, limit, offset]
            );

            const countResult = await pool.query(
                'SELECT COUNT(*) FROM transactions WHERE user_id = $1',
                [player.userId]
            );

            socket.emit('transactions_response', {
                transactions: result.rows,
                total: parseInt(countResult.rows[0].count),
                limit: limit,
                offset: offset,
            });

        } catch (error) {
            logger.error('❌ Transactions error:', error);
            socket.emit('error', { message: 'فشل تحميل المعاملات' });
        }
    });

    // ============================================================
    // 🔔 طلب الإشعارات
    // ============================================================
    socket.on('get_notifications', async () => {
        const player = players.get(socket.id);
        if (!player) return;

        try {
            const result = await pool.query(
                `SELECT * FROM notifications
                 WHERE user_id = $1 AND is_read = FALSE
                 ORDER BY created_at DESC
                 LIMIT 20`,
                [player.userId]
            );

            socket.emit('notifications_response', {
                notifications: result.rows,
            });

        } catch (error) {
            logger.error('❌ Notifications error:', error);
        }
    });

    socket.on('mark_notification_read', async (data) => {
        const player = players.get(socket.id);
        if (!player) return;

        try {
            const { notificationId } = data;
            await pool.query(
                'UPDATE notifications SET is_read = TRUE WHERE id = $1 AND user_id = $2',
                [notificationId, player.userId]
            );

        } catch (error) {
            logger.error('❌ Mark notification error:', error);
        }
    });

    // ============================================================
    // 💓 Ping/Pong
    // ============================================================
    socket.on('ping', (data) => {
        socket.emit('pong', {
            timestamp: data?.timestamp || Date.now(),
            serverTime: Date.now(),
        });
    });

    // ============================================================
    // 🔌 انقطاع الاتصال
    // ============================================================
    socket.on('disconnect', async () => {
        const player = players.get(socket.id);

        if (player) {
            logger.info(`🔌 Disconnected: ${player.username} (${socket.id})`);

            if (player.roomId) {
                try {
                    const roomResult = await pool.query(
                        'SELECT status FROM rooms WHERE id = $1',
                        [player.roomId]
                    );

                    if (roomResult.rows.length > 0) {
                        const status = roomResult.rows[0].status;

                        if (status === 'waiting') {
                            const rpResult = await pool.query(
                                `DELETE FROM room_players
                                 WHERE room_id = $1 AND user_id = $2
                                 RETURNING paid_amount`,
                                [player.roomId, player.userId]
                            );

                            if (rpResult.rows.length > 0) {
                                const paidAmount = rpResult.rows[0].paid_amount || 1;
                                await pool.query(
                                    'UPDATE users SET balance = balance + $1 WHERE id = $2',
                                    [paidAmount, player.userId]
                                );

                                await pool.query(
                                    `INSERT INTO transactions (user_id, type, amount, balance_after, description, room_id)
                                     VALUES ($1, $2, $3, $4, $5, $6)`,
                                    [player.userId, 'refund', paidAmount, player.balance + paidAmount, `مغادرة بسبب الانقطاع`, player.roomId]
                                );
                            }

                            await pool.query(
                                'UPDATE rooms SET players_count = players_count - 1 WHERE id = $1',
                                [player.roomId]
                            );

                            io.to(player.roomId).emit('player_left', {
                                userId: player.userId,
                                username: player.username,
                                reason: 'disconnected',
                            });

                            await sendRoomsListToAll();

                        } else if (status === 'active') {
                            await pool.query(
                                `UPDATE room_players SET is_alive = FALSE, left_at = CURRENT_TIMESTAMP
                                 WHERE room_id = $1 AND user_id = $2`,
                                [player.roomId, player.userId]
                            );

                            io.to(player.roomId).emit('player_left', {
                                userId: player.userId,
                                username: player.username,
                                reason: 'disconnected_during_game',
                            });

                            const aliveResult = await pool.query(
                                `SELECT COUNT(*) FROM room_players
                                 WHERE room_id = $1 AND is_alive = TRUE AND left_at IS NULL`,
                                [player.roomId]
                            );

                            if (parseInt(aliveResult.rows[0].count) <= 1) {
                                await endGame(player.roomId, '🏆 انتهت المعركة بسبب انسحاب لاعب');
                            }
                        }
                    }

                    socket.leave(player.roomId);

                } catch (error) {
                    logger.error('❌ Disconnect cleanup error:', error);
                }
            }

            await pool.query(
                'UPDATE sessions SET socket_id = NULL, last_activity = CURRENT_TIMESTAMP WHERE user_id = $1',
                [player.userId]
            );

            players.delete(socket.id);
            lastMoveTime.delete(socket.id);
            lastShotTime.delete(socket.id);
        }
    });
});

// ================================================================
// 🌐 API Routes المتقدمة
// ================================================================

// ✅ التحقق من صحة الخادم
app.get('/health', (req, res) => {
    res.json({
        status: 'online',
        timestamp: Date.now(),
        version: '6.1.0',
        connections: io.engine.clientsCount,
        rooms: rooms.size,
        activeGames: gameStates.size,
        uptime: process.uptime(),
        memory: {
            rss: process.memoryUsage().rss,
            heapTotal: process.memoryUsage().heapTotal,
            heapUsed: process.memoryUsage().heapUsed,
        },
        db: pool._connected ? 'connected' : 'disconnected',
    });
});

// ✅ الحصول على رصيد المستخدم
app.get('/api/balance/:telegramId', async (req, res) => {
    try {
        const { telegramId } = req.params;
        const result = await pool.query(
            'SELECT balance, username FROM users WHERE telegram_id = $1',
            [telegramId]
        );

        if (result.rows.length === 0) {
            return res.status(404).json({ success: false, error: 'User not found' });
        }

        res.json({
            success: true,
            balance: result.rows[0].balance,
            username: result.rows[0].username,
        });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

// ✅ الحصول على إحصائيات المستخدم
app.get('/api/stats/:telegramId', async (req, res) => {
    try {
        const { telegramId } = req.params;
        const result = await pool.query(
            `SELECT * FROM get_player_stats((SELECT id FROM users WHERE telegram_id = $1))`,
            [telegramId]
        );

        if (result.rows.length === 0) {
            return res.status(404).json({ success: false, error: 'User not found' });
        }

        res.json({
            success: true,
            stats: result.rows[0],
        });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

// ✅ API للمشرفين - إيداع
app.post('/api/admin/deposit', async (req, res) => {
    try {
        const { adminToken, targetTelegramId, amount, reason } = req.body;

        if (adminToken !== process.env.ADMIN_SECRET) {
            return res.status(403).json({ success: false, error: 'Unauthorized' });
        }

        if (!targetTelegramId || !amount || amount <= 0) {
            return res.status(400).json({ success: false, error: 'Invalid parameters' });
        }

        const result = await pool.query(
            `UPDATE users
             SET balance = balance + $1
             WHERE telegram_id = $2
             RETURNING balance, username, id`,
            [amount, targetTelegramId]
        );

        if (result.rows.length === 0) {
            return res.status(404).json({ success: false, error: 'User not found' });
        }

        const user = result.rows[0];
        await pool.query(
            `INSERT INTO transactions (user_id, type, amount, balance_after, description)
             VALUES ($1, $2, $3, $4, $5)`,
            [user.id, 'admin_add', amount, user.balance, reason || 'إيداع من المشرف']
        );

        await logAudit(
            user.id,
            'admin_deposit',
            { target: targetTelegramId, amount, reason },
            req.ip,
            req.headers['user-agent'],
            'warning'
        );

        res.json({
            success: true,
            newBalance: user.balance,
            username: user.username,
        });
    } catch (error) {
        logger.error('❌ Admin deposit error:', error);
        res.status(500).json({ success: false, error: error.message });
    }
});

// ✅ API للمشرفين - حظر
app.post('/api/admin/ban', async (req, res) => {
    try {
        const { adminToken, targetTelegramId, reason, duration } = req.body;

        if (adminToken !== process.env.ADMIN_SECRET) {
            return res.status(403).json({ success: false, error: 'Unauthorized' });
        }

        const userResult = await pool.query(
            'SELECT id FROM users WHERE telegram_id = $1',
            [targetTelegramId]
        );

        if (userResult.rows.length === 0) {
            return res.status(404).json({ success: false, error: 'User not found' });
        }

        const userId = userResult.rows[0].id;
        const expiresAt = duration ? new Date(Date.now() + duration * 1000) : null;

        await pool.query(
            `INSERT INTO bans (user_id, reason, duration, expires_at)
             VALUES ($1, $2, $3, $4)`,
            [userId, reason || 'تم الحظر من قبل المشرف', duration || null, expiresAt]
        );

        await pool.query(
            'UPDATE users SET is_banned = TRUE WHERE id = $1',
            [userId]
        );

        await logAudit(
            userId,
            'admin_ban',
            { target: targetTelegramId, reason, duration },
            req.ip,
            req.headers['user-agent'],
            'critical'
        );

        res.json({
            success: true,
            message: 'تم حظر المستخدم بنجاح',
            expiresAt: expiresAt,
        });
    } catch (error) {
        logger.error('❌ Admin ban error:', error);
        res.status(500).json({ success: false, error: error.message });
    }
});

// ✅ API للمشرفين - فك الحظر
app.post('/api/admin/unban', async (req, res) => {
    try {
        const { adminToken, targetTelegramId } = req.body;

        if (adminToken !== process.env.ADMIN_SECRET) {
            return res.status(403).json({ success: false, error: 'Unauthorized' });
        }

        const userResult = await pool.query(
            'SELECT id FROM users WHERE telegram_id = $1',
            [targetTelegramId]
        );

        if (userResult.rows.length === 0) {
            return res.status(404).json({ success: false, error: 'User not found' });
        }

        const userId = userResult.rows[0].id;

        await pool.query(
            'UPDATE bans SET expires_at = CURRENT_TIMESTAMP WHERE user_id = $1 AND expires_at IS NULL',
            [userId]
        );

        await pool.query(
            'UPDATE users SET is_banned = FALSE WHERE id = $1',
            [userId]
        );

        await logAudit(
            userId,
            'admin_unban',
            { target: targetTelegramId },
            req.ip,
            req.headers['user-agent'],
            'warning'
        );

        res.json({
            success: true,
            message: 'تم فك الحظر عن المستخدم',
        });
    } catch (error) {
        logger.error('❌ Admin unban error:', error);
        res.status(500).json({ success: false, error: error.message });
    }
});

// ✅ API لإحصائيات المشرف
app.get('/api/admin/stats', async (req, res) => {
    try {
        const { adminToken } = req.query;

        if (adminToken !== process.env.ADMIN_SECRET) {
            return res.status(403).json({ success: false, error: 'Unauthorized' });
        }

        const stats = await pool.query(`
            SELECT
                (SELECT COUNT(*) FROM users) as total_users,
                (SELECT COUNT(*) FROM users WHERE is_admin = TRUE) as admin_count,
                (SELECT COUNT(*) FROM users WHERE is_banned = TRUE) as banned_count,
                (SELECT SUM(balance) FROM users) as total_balance,
                (SELECT SUM(games_played) FROM users) as total_games,
                (SELECT SUM(wins) FROM users) as total_wins,
                (SELECT COUNT(*) FROM rooms WHERE status = 'active') as active_games,
                (SELECT COUNT(*) FROM rooms WHERE status = 'waiting') as waiting_games,
                (SELECT COUNT(*) FROM transactions WHERE created_at > CURRENT_DATE) as today_transactions,
                (SELECT SUM(amount) FROM transactions WHERE type = 'game_entry' AND created_at > CURRENT_DATE) as today_revenue
        `);

        const users = await pool.query(
            `SELECT id, telegram_id, username, balance, games_played, wins, total_kills, is_admin, is_banned, created_at
             FROM users ORDER BY created_at DESC LIMIT 100`
        );

        res.json({
            success: true,
            stats: stats.rows[0],
            users: users.rows,
        });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

// ✅ API لقائمة الغرف (للمشرف)
app.get('/api/admin/rooms', async (req, res) => {
    try {
        const { adminToken } = req.query;

        if (adminToken !== process.env.ADMIN_SECRET) {
            return res.status(403).json({ success: false, error: 'Unauthorized' });
        }

        const roomsList = await pool.query(
            `SELECT r.*, COUNT(rp.id) as actual_players,
                    array_agg(DISTINCT u.username) as player_names
             FROM rooms r
             LEFT JOIN room_players rp ON r.id = rp.room_id AND rp.left_at IS NULL
             LEFT JOIN users u ON rp.user_id = u.id
             GROUP BY r.id
             ORDER BY r.created_at DESC`
        );

        res.json({
            success: true,
            rooms: roomsList.rows,
        });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

// ✅ API لتحديث إعدادات الغرف
app.post('/api/admin/update_room_type', async (req, res) => {
    try {
        const { adminToken, typeName, maxSeats, seatPrice } = req.body;

        if (adminToken !== process.env.ADMIN_SECRET) {
            return res.status(403).json({ success: false, error: 'Unauthorized' });
        }

        if (!typeName || !maxSeats || maxSeats < 2 || maxSeats > 16 || !seatPrice || seatPrice < 1) {
            return res.status(400).json({ success: false, error: 'Invalid parameters' });
        }

        await pool.query(
            `UPDATE rooms SET max_seats = $1, seat_price = $2 WHERE type = $3`,
            [maxSeats, seatPrice, typeName]
        );

        for (const [id, room] of rooms) {
            if (room.type === typeName) {
                room.maxSeats = maxSeats;
                room.seatPrice = seatPrice;
            }
        }

        await logAudit(
            null,
            'admin_update_room_type',
            { typeName, maxSeats, seatPrice },
            req.ip,
            req.headers['user-agent'],
            'warning'
        );

        await sendRoomsListToAll();

        res.json({
            success: true,
            message: `تم تحديث ${typeName} إلى ${maxSeats} لاعبين و ${seatPrice}$`,
        });
    } catch (error) {
        logger.error('❌ Update room type error:', error);
        res.status(500).json({ success: false, error: error.message });
    }
});

// ================================================================
// 🚀 مسار اللعبة
// ================================================================
app.get('/game', (req, res) => {
    const { user_id } = req.query;
    if (!user_id) {
        return res.sendFile(path.join(__dirname, 'public', 'index.html'));
    }
    res.sendFile(path.join(__dirname, 'public', 'game.html'));
});

// ================================================================
// 🚀 تشغيل الخادم
// ================================================================
const PORT = process.env.PORT || 3000;
let isDbReady = false;

async function startServer() {
    try {
        await initializeDatabase();
        isDbReady = true;

        await initializeRooms();

        server.listen(PORT, () => {
            console.log(`
╔═══════════════════════════════════════════════════════════════════════════╗
║                                                                           ║
║           🎮 BATTLE TANKS ELITE v6.1 - READY FOR PRODUCTION 🎮           ║
║                                                                           ║
╠═══════════════════════════════════════════════════════════════════════════╣
║  📡 Server:        http://localhost:${PORT}
║  🌐 WebSocket:     wss://localhost:${PORT}
║  🗄️  PostgreSQL:   Connected (Neon)
║  🏠 Rooms:         ${rooms.size} rooms (${ROOM_TYPES.length} types × ${ROOMS_PER_TYPE} each)
║  👥 Connections:   ${io.engine.clientsCount}
║  ⏱️  Game Duration: ${parseInt(process.env.GAME_DURATION) / 1000 || 300} seconds
║  🚀 Version:       6.1.0
║  🔐 Security:      JWT + Rate Limit + Slow Down + Helmet
║  📊 Queue System:  Active
║                                                                           ║
╚═══════════════════════════════════════════════════════════════════════════╝
            `);
        });

    } catch (error) {
        logger.error('❌ Failed to start server:', error);
        process.exit(1);
    }
}

// ================================================================
// 🛑 التعامل مع الإغلاق الآمن
// ================================================================
async function shutdown() {
    logger.info('🛑 Shutting down gracefully...');

    for (const [roomId, timer] of roomTimers) {
        clearTimeout(timer);
    }
    roomTimers.clear();

    for (const [roomId, interval] of roomIntervals) {
        clearInterval(interval);
    }
    roomIntervals.clear();

    actionQueue.clear();

    io.close(() => {
        logger.info('✅ Socket.io closed');
    });

    await pool.end();
    logger.info('✅ Database connection closed');

    process.exit(0);
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
process.on('uncaughtException', (error) => {
    logger.error('❌ Uncaught exception:', error);
});

process.on('unhandledRejection', (reason, promise) => {
    logger.error('❌ Unhandled rejection:', reason);
});

// ================================================================
// 🚀 بدء التشغيل
// ================================================================
startServer();

module.exports = { app, server, io, pool };
