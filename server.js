// ============================================
// 🚀 خادم Battle Tanks Royale - الإصدار النهائي المتكامل
// ============================================
// Version: 9.0.0
// Auth: Telegram ID based (URL Parameter)
// Database: PostgreSQL (Neon) with DATABASE_URL env
// Features: 
//   - نظام متين لمنع انقطاع الاتصال
//   - إدارة مستخدمين متكاملة
//   - نظام إدارة متقدم للآدمن
//   - حماية متطورة ضد الهجمات
//   - طوابير معالجة ذكية
//   - نظام مراقبة وتحليل متقدم
// ============================================

const express = require('express');
const http = require('http');
const socketIo = require('socket.io');
const { Pool } = require('pg');
const cors = require('cors');
const crypto = require('crypto');
const EventEmitter = require('events');

// ============================================
// 📊 نظام المراقبة والتحليل المتقدم
// ============================================
class MonitoringSystem {
    constructor() {
        this.metrics = {
            connections: { total: 0, active: 0, peak: 0 },
            requests: { total: 0, success: 0, error: 0, rate: 0 },
            games: { total: 0, active: 0, completed: 0 },
            errors: { total: 0, byType: {} },
            performance: { avgResponseTime: 0, maxResponseTime: 0 },
            database: { connected: false, reconnectAttempts: 0, lastError: null },
            admin: { logins: 0, actions: 0 }
        };
        this.startTime = Date.now();
        this.requestTimestamps = [];
        this.errorLogs = [];
        this.maxErrorLogs = 100;
    }

    recordConnection(type) {
        if (type === 'connect') {
            this.metrics.connections.active++;
            this.metrics.connections.total++;
            if (this.metrics.connections.active > this.metrics.connections.peak) {
                this.metrics.connections.peak = this.metrics.connections.active;
            }
        } else if (type === 'disconnect') {
            this.metrics.connections.active = Math.max(0, this.metrics.connections.active - 1);
        }
    }

    recordRequest(success, duration) {
        this.metrics.requests.total++;
        if (success) {
            this.metrics.requests.success++;
        } else {
            this.metrics.requests.error++;
        }
        
        this.requestTimestamps.push(Date.now());
        if (this.requestTimestamps.length > 1000) {
            this.requestTimestamps.shift();
        }
        
        const oneMinuteAgo = Date.now() - 60000;
        const recentRequests = this.requestTimestamps.filter(t => t > oneMinuteAgo);
        this.metrics.requests.rate = recentRequests.length / 60;

        if (duration) {
            const avg = (this.metrics.performance.avgResponseTime * 0.9) + (duration * 0.1);
            this.metrics.performance.avgResponseTime = Math.round(avg);
            if (duration > this.metrics.performance.maxResponseTime) {
                this.metrics.performance.maxResponseTime = Math.round(duration);
            }
        }
    }

    recordError(errorType, errorDetails = null) {
        this.metrics.errors.total++;
        if (!this.metrics.errors.byType[errorType]) {
            this.metrics.errors.byType[errorType] = 0;
        }
        this.metrics.errors.byType[errorType]++;
        
        const errorLog = {
            type: errorType,
            timestamp: Date.now(),
            details: errorDetails
        };
        this.errorLogs.push(errorLog);
        if (this.errorLogs.length > this.maxErrorLogs) {
            this.errorLogs.shift();
        }
    }

    recordDatabaseStatus(connected, error = null) {
        this.metrics.database.connected = connected;
        if (error) {
            this.metrics.database.lastError = error;
            this.metrics.database.reconnectAttempts++;
        } else {
            this.metrics.database.reconnectAttempts = 0;
            this.metrics.database.lastError = null;
        }
    }

    recordAdminAction(type) {
        if (type === 'login') {
            this.metrics.admin.logins++;
        } else {
            this.metrics.admin.actions++;
        }
    }

    recordGameStarted() {
        this.metrics.games.total++;
        this.metrics.games.active++;
    }

    recordGameEnded() {
        this.metrics.games.active = Math.max(0, this.metrics.games.active - 1);
        this.metrics.games.completed++;
    }

    getStats() {
        const uptime = Math.floor((Date.now() - this.startTime) / 1000);
        return {
            ...this.metrics,
            uptime,
            uptimeFormatted: this.formatUptime(uptime),
            errorLogs: this.errorLogs.slice(-10)
        };
    }

    formatUptime(seconds) {
        const days = Math.floor(seconds / 86400);
        const hours = Math.floor((seconds % 86400) / 3600);
        const minutes = Math.floor((seconds % 3600) / 60);
        const secs = seconds % 60;
        return `${days}d ${hours}h ${minutes}m ${secs}s`;
    }

    getHealthStatus() {
        return {
            status: this.metrics.database.connected ? 'healthy' : 'degraded',
            uptime: this.formatUptime(Math.floor((Date.now() - this.startTime) / 1000)),
            connections: this.metrics.connections.active,
            database: this.metrics.database,
            errors: {
                total: this.metrics.errors.total,
                recent: this.errorLogs.slice(-5)
            }
        };
    }
}

// ============================================
// 🔒 نظام القفل المتقدم
// ============================================
class AdvancedLockSystem {
    constructor() {
        this.locks = new Map();
        this.waitingQueues = new Map();
        this.lockTimeouts = new Map();
        this.maxLockTime = 30000;
        this.cleanupInterval = setInterval(() => this.cleanup(), 60000);
    }

    async acquireLock(resourceId, userId, timeout = 10000) {
        return new Promise((resolve, reject) => {
            const lockKey = `${resourceId}:${userId}`;
            
            if (this.locks.has(lockKey)) {
                if (!this.waitingQueues.has(lockKey)) {
                    this.waitingQueues.set(lockKey, []);
                }
                
                const queue = this.waitingQueues.get(lockKey);
                const timeoutId = setTimeout(() => {
                    const index = queue.findIndex(item => item.userId === userId);
                    if (index !== -1) {
                        queue.splice(index, 1);
                        reject(new Error('Lock acquisition timeout'));
                    }
                }, timeout);
                
                queue.push({ userId, resolve, reject, timeoutId });
                return;
            }

            this.locks.set(lockKey, {
                userId,
                acquiredAt: Date.now(),
                expiresAt: Date.now() + this.maxLockTime
            });

            const timeoutId = setTimeout(() => {
                this.releaseLock(resourceId, userId);
            }, this.maxLockTime);
            
            this.lockTimeouts.set(lockKey, timeoutId);
            resolve(true);
        });
    }

    releaseLock(resourceId, userId) {
        const lockKey = `${resourceId}:${userId}`;
        
        this.locks.delete(lockKey);
        
        if (this.lockTimeouts.has(lockKey)) {
            clearTimeout(this.lockTimeouts.get(lockKey));
            this.lockTimeouts.delete(lockKey);
        }

        if (this.waitingQueues.has(lockKey)) {
            const queue = this.waitingQueues.get(lockKey);
            if (queue.length > 0) {
                const next = queue.shift();
                clearTimeout(next.timeoutId);
                this.locks.set(lockKey, {
                    userId: next.userId,
                    acquiredAt: Date.now(),
                    expiresAt: Date.now() + this.maxLockTime
                });
                
                const timeoutId = setTimeout(() => {
                    this.releaseLock(resourceId, next.userId);
                }, this.maxLockTime);
                this.lockTimeouts.set(lockKey, timeoutId);
                
                next.resolve(true);
            } else {
                this.waitingQueues.delete(lockKey);
            }
        }
    }

    isLocked(resourceId, userId) {
        const lockKey = `${resourceId}:${userId}`;
        return this.locks.has(lockKey);
    }

    getLockInfo(resourceId, userId) {
        const lockKey = `${resourceId}:${userId}`;
        if (this.locks.has(lockKey)) {
            return this.locks.get(lockKey);
        }
        return null;
    }

    cleanup() {
        const now = Date.now();
        for (const [key, lock] of this.locks) {
            if (lock.expiresAt < now) {
                const [resourceId, userId] = key.split(':');
                this.releaseLock(resourceId, userId);
            }
        }
    }

    forceUnlock(resourceId, userId) {
        const lockKey = `${resourceId}:${userId}`;
        if (this.locks.has(lockKey)) {
            this.releaseLock(resourceId, userId);
            return true;
        }
        return false;
    }

    getStats() {
        return {
            activeLocks: this.locks.size,
            waitingQueues: this.waitingQueues.size,
            totalWaiting: Array.from(this.waitingQueues.values()).reduce((sum, q) => sum + q.length, 0)
        };
    }
}

// ============================================
// 🛡️ نظام الحماية المتقدم
// ============================================
class AntiCheatSystem {
    constructor() {
        this.actionTracker = new Map();
        this.rateLimits = {
            move: { max: 60, window: 1000 },
            shoot: { max: 2, window: 3000 },
            join: { max: 5, window: 10000 },
            auth: { max: 5, window: 5000 },
            admin: { max: 5, window: 60000 }
        };
        this.suspiciousActivity = new Map();
        this.bannedUsers = new Set();
        this.enabled = true;
        this.adminPassword = process.env.ADMIN_PASSWORD || 'Admin@2024#Battle';
    }

    checkRateLimit(userId, actionType) {
        if (!this.enabled) return true;
        
        const now = Date.now();
        const limit = this.rateLimits[actionType];
        if (!limit) return true;

        if (!this.actionTracker.has(userId)) {
            this.actionTracker.set(userId, { actions: [], lastReset: now });
        }

        const tracker = this.actionTracker.get(userId);
        
        if (now - tracker.lastReset > limit.window) {
            tracker.actions = [];
            tracker.lastReset = now;
        }

        tracker.actions.push(now);
        tracker.actions = tracker.actions.filter(t => now - t < limit.window);

        if (tracker.actions.length > limit.max) {
            this.reportSuspiciousActivity(userId, `Rate limit exceeded: ${actionType}`);
            return false;
        }

        return true;
    }

    reportSuspiciousActivity(userId, reason) {
        if (!this.suspiciousActivity.has(userId)) {
            this.suspiciousActivity.set(userId, {
                reports: [],
                warnings: 0,
                lastReport: Date.now()
            });
        }

        const activity = this.suspiciousActivity.get(userId);
        activity.reports.push({ reason, timestamp: Date.now() });
        activity.warnings++;
        activity.lastReport = Date.now();

        if (activity.warnings >= 10) {
            this.banUser(userId, 'مشتبه به: نشاط غير طبيعي متكرر');
        }

        console.warn(`⚠️ Suspicious activity detected: ${userId} - ${reason}`);
    }

    async banUser(userId, reason) {
        this.bannedUsers.add(userId);
        console.log(`🚫 User banned: ${userId} - ${reason}`);
        
        try {
            if (pool) {
                await pool.query(
                    `UPDATE users SET 
                     is_banned = TRUE,
                     ban_reason = $1,
                     banned_until = $2
                     WHERE id = $3`,
                    [reason, new Date(Date.now() + 24 * 60 * 60 * 1000), userId]
                );
            }
        } catch (error) {
            console.error('Error banning user in database:', error);
        }
    }

    async isUserBanned(userId) {
        if (this.bannedUsers.has(userId)) return true;

        try {
            if (pool) {
                const result = await pool.query(
                    'SELECT is_banned, banned_until FROM users WHERE id = $1',
                    [userId]
                );
                if (result.rows.length > 0) {
                    const user = result.rows[0];
                    if (user.is_banned && user.banned_until && new Date(user.banned_until) > new Date()) {
                        this.bannedUsers.add(userId);
                        return true;
                    }
                }
            }
        } catch (error) {
            console.error('Error checking ban status:', error);
        }
        return false;
    }

    verifyAdminPassword(password) {
        return password === this.adminPassword;
    }

    getStats() {
        return {
            bannedUsers: this.bannedUsers.size,
            suspiciousActivities: this.suspiciousActivity.size,
            activeTrackers: this.actionTracker.size,
            enabled: this.enabled
        };
    }
}

// ============================================
// 🔥 تهيئة Express
// ============================================
const app = express();
app.use(cors({
    origin: '*',
    methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
    credentials: true,
    allowedHeaders: ['Content-Type', 'Authorization', 'X-Admin-Token']
}));
app.use(express.json({ limit: '10mb' }));

// ============================================
// 🗄️ اتصال PostgreSQL مع إعادة محاولة ذكية
// ============================================
class DatabaseManager {
    constructor() {
        this.pool = null;
        this.isConnected = false;
        this.reconnectAttempts = 0;
        this.maxReconnectAttempts = Infinity;
        this.reconnectDelay = 5000;
        this.maxReconnectDelay = 60000;
        this.reconnectTimer = null;
        this.isReconnecting = false;
        this.connectionString = process.env.DATABASE_URL;
        this.lastError = null;
        this.pingInterval = null;
        
        if (!this.connectionString) {
            console.error('❌ DATABASE_URL environment variable is not set!');
            this.connectionString = 'postgresql://neondb_owner:npg_MSOwr97htVJu@ep-patient-dawn-awed2uh0-pooler.c-12.us-east-1.aws.neon.tech/neondb?sslmode=require';
        }
        
        console.log('📡 Database URL configured:', this.connectionString ? '✅ Yes' : '❌ No');
    }

    async connect() {
        if (this.isReconnecting) {
            console.log('🔄 Reconnection already in progress, waiting...');
            return new Promise((resolve) => {
                const checkInterval = setInterval(() => {
                    if (!this.isReconnecting) {
                        clearInterval(checkInterval);
                        resolve(this.pool);
                    }
                }, 500);
            });
        }

        try {
            console.log(`📡 Connecting to database (attempt ${this.reconnectAttempts + 1})...`);
            
            this.pool = new Pool({
                connectionString: this.connectionString,
                ssl: { rejectUnauthorized: false },
                max: 30,
                idleTimeoutMillis: 30000,
                connectionTimeoutMillis: 15000,
                retryDelay: this.reconnectDelay,
            });

            // اختبار الاتصال مع مهلة
            const timeoutPromise = new Promise((_, reject) => {
                setTimeout(() => reject(new Error('Connection timeout')), 20000);
            });

            const connectPromise = this.pool.connect();
            const client = await Promise.race([connectPromise, timeoutPromise]);
            client.release();
            
            this.isConnected = true;
            this.reconnectAttempts = 0;
            this.isReconnecting = false;
            this.reconnectDelay = 5000;
            this.lastError = null;
            monitoring.recordDatabaseStatus(true);
            
            // بدء فحص الصحة الدوري
            if (this.pingInterval) clearInterval(this.pingInterval);
            this.pingInterval = setInterval(() => this.healthCheck(), 30000);
            
            console.log('✅ PostgreSQL connected successfully');
            return this.pool;
            
        } catch (error) {
            console.error('❌ PostgreSQL connection failed:', error.message);
            this.isConnected = false;
            this.isReconnecting = false;
            this.lastError = error.message;
            monitoring.recordDatabaseStatus(false, error.message);
            monitoring.recordError('database_connection_error', error.message);
            
            return this.handleReconnect();
        }
    }

    async healthCheck() {
        try {
            if (this.pool) {
                await this.pool.query('SELECT 1');
                if (!this.isConnected) {
                    console.log('✅ Database health check: recovered');
                    this.isConnected = true;
                    monitoring.recordDatabaseStatus(true);
                }
            }
        } catch (error) {
            if (this.isConnected) {
                console.error('❌ Database health check failed:', error.message);
                this.isConnected = false;
                monitoring.recordDatabaseStatus(false, error.message);
                this.handleReconnect();
            }
        }
    }

    async handleReconnect() {
        if (this.isReconnecting) return this.pool;
        
        this.isReconnecting = true;
        this.reconnectAttempts++;
        
        const delay = Math.min(this.reconnectDelay * Math.pow(1.5, this.reconnectAttempts - 1), this.maxReconnectDelay);
        
        console.log(`🔄 Reconnecting attempt ${this.reconnectAttempts} in ${Math.round(delay/1000)}s...`);
        
        return new Promise((resolve) => {
            if (this.reconnectTimer) {
                clearTimeout(this.reconnectTimer);
            }
            
            this.reconnectTimer = setTimeout(async () => {
                console.log(`🔄 Attempting reconnection ${this.reconnectAttempts}...`);
                try {
                    await this.connect();
                    resolve(this.pool);
                } catch (error) {
                    console.error(`❌ Reconnection ${this.reconnectAttempts} failed:`, error.message);
                    this.isReconnecting = false;
                    this.handleReconnect().then(resolve);
                }
            }, delay);
        });
    }

    async query(text, params) {
        if (!this.isConnected || !this.pool) {
            console.log('⏳ Waiting for database connection...');
            await this.connect();
        }

        try {
            return await this.pool.query(text, params);
        } catch (error) {
            console.error('Database query error:', error);
            
            if (error.code === 'ECONNRESET' || 
                error.code === '57P01' || 
                error.code === '08003' ||
                error.code === '08006' ||
                error.message.includes('connection')) {
                
                console.log('🔄 Connection lost, attempting to reconnect...');
                this.isConnected = false;
                monitoring.recordDatabaseStatus(false, error.message);
                
                try {
                    await this.connect();
                    console.log('🔄 Reconnected, retrying query...');
                    return await this.pool.query(text, params);
                } catch (reconnectError) {
                    console.error('❌ Reconnection failed for query:', reconnectError.message);
                    throw new Error(`Database connection lost and reconnection failed: ${reconnectError.message}`);
                }
            }
            
            throw error;
        }
    }

    async transaction(callback) {
        if (!this.isConnected || !this.pool) {
            await this.connect();
        }
        
        const client = await this.pool.connect();
        try {
            await client.query('BEGIN');
            const result = await callback(client);
            await client.query('COMMIT');
            return result;
        } catch (error) {
            await client.query('ROLLBACK');
            throw error;
        } finally {
            client.release();
        }
    }

    getHealth() {
        return {
            connected: this.isConnected,
            reconnectAttempts: this.reconnectAttempts,
            poolSize: this.pool ? this.pool.totalCount : 0,
            idleCount: this.pool ? this.pool.idleCount : 0,
            waitingCount: this.pool ? this.pool.waitingCount : 0,
            lastError: this.lastError
        };
    }
}

// ============================================
// 📦 أنظمة الحماية
// ============================================
const lockSystem = new AdvancedLockSystem();
const antiCheat = new AntiCheatSystem();
const monitoring = new MonitoringSystem();

// ============================================
// 📦 التخزين المؤقت
// ============================================
const players = new Map();
const rooms = new Map();
const activeGames = new Map();
const pendingReconnects = new Map();
const leaderboardCache = new Map();
const visualSettingsCache = new Map();
const userCache = new Map();

// ============================================
// 🔧 تهيئة قاعدة البيانات مع إعادة محاولة
// ============================================
async function initializeDatabase(retryCount = 0) {
    try {
        const maxRetries = 5;
        
        await pool.query(`
            CREATE TABLE IF NOT EXISTS users (
                id VARCHAR(255) PRIMARY KEY,
                telegram_id VARCHAR(255) UNIQUE NOT NULL,
                username VARCHAR(100),
                first_name VARCHAR(100),
                last_name VARCHAR(100),
                photo_url TEXT,
                balance DECIMAL(10,2) DEFAULT 0,
                elo INTEGER DEFAULT 1000,
                kills INTEGER DEFAULT 0,
                wins INTEGER DEFAULT 0,
                games_played INTEGER DEFAULT 0,
                total_rewards DECIMAL(10,2) DEFAULT 0,
                is_admin BOOLEAN DEFAULT FALSE,
                is_banned BOOLEAN DEFAULT FALSE,
                banned_until TIMESTAMP,
                ban_reason TEXT,
                last_ip VARCHAR(45),
                device_id VARCHAR(255),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_login TIMESTAMP,
                last_game TIMESTAMP,
                login_count INTEGER DEFAULT 0
            )
        `);

        await pool.query(`
            CREATE TABLE IF NOT EXISTS server_config (
                key VARCHAR(100) PRIMARY KEY,
                value JSONB NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        `);

        await pool.query(`
            CREATE TABLE IF NOT EXISTS game_settings (
                id SERIAL PRIMARY KEY,
                setting_key VARCHAR(50) UNIQUE NOT NULL,
                value JSONB NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        `);

        await pool.query(`
            CREATE TABLE IF NOT EXISTS rooms (
                id VARCHAR(50) PRIMARY KEY,
                type VARCHAR(50) NOT NULL,
                name VARCHAR(100) NOT NULL,
                max_seats INTEGER NOT NULL,
                seat_price DECIMAL(10,2) NOT NULL,
                reward_multiplier DECIMAL(3,2) DEFAULT 1.00,
                status VARCHAR(20) DEFAULT 'waiting',
                players JSONB DEFAULT '[]',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                game_round INTEGER DEFAULT 0
            )
        `);

        await pool.query(`
            CREATE TABLE IF NOT EXISTS matches (
                id VARCHAR(50) PRIMARY KEY,
                room_id VARCHAR(50) REFERENCES rooms(id),
                winner_id VARCHAR(255) REFERENCES users(id),
                players JSONB NOT NULL,
                kill_feed JSONB DEFAULT '[]',
                start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                end_time TIMESTAMP,
                duration INTEGER,
                total_players INTEGER,
                game_round INTEGER DEFAULT 0
            )
        `);

        await pool.query(`
            CREATE TABLE IF NOT EXISTS visual_settings (
                id SERIAL PRIMARY KEY,
                event_key VARCHAR(50) UNIQUE NOT NULL,
                image_url TEXT NOT NULL,
                alt_text VARCHAR(255),
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        `);

        await pool.query(`
            CREATE TABLE IF NOT EXISTS event_log (
                id SERIAL PRIMARY KEY,
                event_type VARCHAR(50) NOT NULL,
                user_id VARCHAR(255) REFERENCES users(id),
                details JSONB,
                ip_address VARCHAR(45),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        `);

        await pool.query(`
            CREATE TABLE IF NOT EXISTS processed_actions (
                id SERIAL PRIMARY KEY,
                action_id VARCHAR(255) UNIQUE NOT NULL,
                user_id VARCHAR(255) REFERENCES users(id),
                action_type VARCHAR(50),
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                expires_at TIMESTAMP
            )
        `);

        await pool.query(`
            CREATE INDEX IF NOT EXISTS idx_users_telegram_id ON users(telegram_id);
            CREATE INDEX IF NOT EXISTS idx_rooms_status ON rooms(status);
            CREATE INDEX IF NOT EXISTS idx_matches_winner ON matches(winner_id);
            CREATE INDEX IF NOT EXISTS idx_processed_actions_action_id ON processed_actions(action_id);
        `);

        // تهيئة الإعدادات البصرية الافتراضية
        const defaultImages = [
            { key: 'game_logo', url: '/images/default/logo.png', alt: 'شعار اللعبة' },
            { key: 'elimination', url: '/images/default/elimination.png', alt: 'تم إقصاؤك' },
            { key: 'kill', url: '/images/default/kill.png', alt: 'أقصيت لاعباً' },
            { key: 'winner', url: '/images/default/winner.png', alt: 'فائز' },
            { key: 'game_start', url: '/images/default/game_start.png', alt: 'بدء المعركة' },
            { key: 'game_end', url: '/images/default/game_end.png', alt: 'انتهت المعركة' },
            { key: 'level_up', url: '/images/default/level_up.png', alt: 'تقدم في المستوى' },
            { key: 'achievement', url: '/images/default/achievement.png', alt: 'إنجاز جديد' },
            { key: 'profile_background', url: '/images/default/profile_bg.jpg', alt: 'خلفية الملف الشخصي' }
        ];

        for (const img of defaultImages) {
            await pool.query(
                `INSERT INTO visual_settings (event_key, image_url, alt_text) 
                 VALUES ($1, $2, $3) 
                 ON CONFLICT (event_key) DO UPDATE SET 
                 image_url = EXCLUDED.image_url, 
                 alt_text = EXCLUDED.alt_text`,
                [img.key, img.url, img.alt]
            );
        }

        // تهيئة إعدادات الخادم
        const defaultConfig = {
            rooms: {
                beginner: { enabled: true, maxSeats: 8, seatPrice: 1, rewardMultiplier: 1, maxRooms: 5 },
                advanced: { enabled: true, maxSeats: 12, seatPrice: 5, rewardMultiplier: 2, maxRooms: 3 },
                pro: { enabled: true, maxSeats: 16, seatPrice: 10, rewardMultiplier: 3, maxRooms: 2 }
            },
            game: {
                duration: 300000,
                mapSize: 600,
                boundaryLimit: 280,
                bulletSpeed: 2.6,
                bulletDamage: 100,
                fireCooldown: 3000,
                tankHealth: 100,
                respawnTime: 5000,
                killRewardPercent: 0.8,
                winRewardMultiplier: 1.5,
                minPlayersToStart: 2
            },
            system: {
                maintenanceMode: false,
                maxPlayersPerMatch: 16,
                leaderboardCacheTime: 30000,
                reconnectTimeout: 30000,
                antiCheatEnabled: true,
                maxLoginAttempts: 5,
                lockTimeout: 30000,
                adminPassword: 'Admin@2024#Battle'
            },
            appearance: {
                gameLogo: '/images/default/logo.png',
                backgroundImage: '/images/default/background.jpg'
            }
        };

        await pool.query(
            `INSERT INTO server_config (key, value) 
             VALUES ('server_config', $1) 
             ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value`,
            [defaultConfig]
        );

        console.log('✅ Database initialized successfully');
        
        // تحميل الإعدادات
        await loadVisualSettings();
        await loadGameSettings();
        await initializeRooms();
        
        return true;
        
    } catch (error) {
        console.error('❌ Database initialization error:', error);
        monitoring.recordError('database_init_error', error.message);
        
        if (retryCount < 5) {
            const delay = Math.min(5000 * Math.pow(2, retryCount), 30000);
            console.log(`🔄 Retrying database initialization in ${delay/1000}s (attempt ${retryCount + 1}/5)...`);
            await new Promise(resolve => setTimeout(resolve, delay));
            return initializeDatabase(retryCount + 1);
        }
        
        throw error;
    }
}

// ============================================
// 🎮 تحميل إعدادات اللعبة
// ============================================
async function loadGameSettings() {
    try {
        const result = await pool.query(
            "SELECT value FROM server_config WHERE key = 'server_config'"
        );
        const config = result.rows[0]?.value || {};
        
        // تحديث كلمة مرور الآدمن من المتغيرات البيئية
        if (process.env.ADMIN_PASSWORD) {
            config.system.adminPassword = process.env.ADMIN_PASSWORD;
            await pool.query(
                `UPDATE server_config SET value = $1 WHERE key = 'server_config'`,
                [config]
            );
        }
        
        antiCheat.adminPassword = config.system.adminPassword || 'Admin@2024#Battle';
        return config;
    } catch (error) {
        console.error('❌ Error loading game settings:', error);
        return {};
    }
}

// ============================================
// 🖼️ تحميل الإعدادات البصرية مع إعادة محاولة
// ============================================
async function loadVisualSettings(retryCount = 0) {
    try {
        const result = await pool.query('SELECT * FROM visual_settings');
        visualSettingsCache.clear();
        result.rows.forEach(row => {
            visualSettingsCache.set(row.event_key, {
                imageUrl: row.image_url,
                altText: row.alt_text
            });
        });
        console.log(`✅ Loaded ${visualSettingsCache.size} visual settings`);
    } catch (error) {
        console.error('❌ Error loading visual settings:', error);
        if (retryCount < 3) {
            console.log(`🔄 Retrying loadVisualSettings (${retryCount + 1}/3)...`);
            await new Promise(resolve => setTimeout(resolve, 2000));
            return loadVisualSettings(retryCount + 1);
        }
        throw error;
    }
}

// ============================================
// 🏠 تهيئة الغرف مع قفل
// ============================================
async function initializeRooms() {
    const lockKey = 'rooms_initialization';
    try {
        await lockSystem.acquireLock(lockKey, 'system', 10000);
        
        const configResult = await pool.query(
            "SELECT value FROM server_config WHERE key = 'server_config'"
        );
        const config = configResult.rows[0]?.value || {};
        const roomConfigs = config.rooms || {};
        
        rooms.clear();
        
        for (const [type, settings] of Object.entries(roomConfigs)) {
            if (!settings.enabled) continue;
            
            const typeNames = {
                beginner: 'غرفة المبتدئين',
                advanced: 'غرفة المتقدمين',
                pro: 'غرفة المحترفين'
            };
            
            for (let i = 1; i <= settings.maxRooms; i++) {
                const roomId = `${type}_room_${i}`;
                const room = {
                    id: roomId,
                    type: type,
                    name: `${typeNames[type] || type} ${i}`,
                    maxSeats: settings.maxSeats,
                    seatPrice: settings.seatPrice,
                    rewardMultiplier: settings.rewardMultiplier || 1,
                    players: [],
                    status: 'waiting',
                    createdAt: Date.now(),
                    startTime: null,
                    roomNumber: i,
                    gameRound: 0
                };
                rooms.set(roomId, room);
                
                await pool.query(
                    `INSERT INTO rooms (id, type, name, max_seats, seat_price, reward_multiplier, status, players, game_round)
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                     ON CONFLICT (id) DO UPDATE SET
                     type = EXCLUDED.type,
                     name = EXCLUDED.name,
                     max_seats = EXCLUDED.max_seats,
                     seat_price = EXCLUDED.seat_price,
                     reward_multiplier = EXCLUDED.reward_multiplier,
                     status = EXCLUDED.status,
                     game_round = EXCLUDED.game_round`,
                    [roomId, type, room.name, room.maxSeats, room.seatPrice, room.rewardMultiplier, 'waiting', '[]', 0]
                );
            }
        }
        
        console.log(`✅ Initialized ${rooms.size} rooms`);
        broadcastRoomsList();
        broadcastLobbyInfo();
        
        lockSystem.releaseLock(lockKey, 'system');
    } catch (error) {
        console.error('❌ Error initializing rooms:', error);
        lockSystem.releaseLock(lockKey, 'system');
        throw error;
    }
}

// ============================================
// 🔌 معالج الطوابير
// ============================================
class ActionQueueProcessor {
    constructor() {
        this.queue = [];
        this.processing = false;
        this.maxConcurrent = 10;
        this.timeout = 30000;
        this.processedCount = 0;
        this.errorCount = 0;
    }

    async add(action) {
        return new Promise((resolve, reject) => {
            const item = {
                id: `action_${Date.now()}_${Math.random().toString(36).substr(2, 6)}`,
                action,
                resolve,
                reject,
                timestamp: Date.now(),
                retries: 0,
                maxRetries: 3
            };
            this.queue.push(item);
            this.process();
        });
    }

    async process() {
        if (this.processing || this.queue.length === 0) return;
        
        this.processing = true;
        
        while (this.queue.length > 0) {
            const batch = this.queue.splice(0, this.maxConcurrent);
            await Promise.all(batch.map(item => this.processItem(item)));
        }
        
        this.processing = false;
    }

    async processItem(item) {
        try {
            if (Date.now() - item.timestamp > this.timeout) {
                item.reject(new Error('Action timeout'));
                return;
            }

            const result = await item.action();
            this.processedCount++;
            item.resolve(result);
        } catch (error) {
            console.error('Action processing error:', error);
            this.errorCount++;
            
            if (item.retries < item.maxRetries) {
                item.retries++;
                item.timestamp = Date.now();
                this.queue.push(item);
                console.log(`🔄 Retrying action ${item.id} (${item.retries}/${item.maxRetries})`);
            } else {
                item.reject(error);
            }
        }
    }

    getStats() {
        return {
            queueLength: this.queue.length,
            processing: this.processing,
            maxConcurrent: this.maxConcurrent,
            processedCount: this.processedCount,
            errorCount: this.errorCount
        };
    }
}

const actionProcessor = new ActionQueueProcessor();

// ============================================
// 🌐 خادم HTTP و WebSocket
// ============================================
const server = http.createServer(app);
const io = socketIo(server, {
    cors: {
        origin: "*",
        methods: ["GET", "POST"],
        credentials: true
    },
    transports: ['websocket', 'polling'],
    pingTimeout: 60000,
    pingInterval: 25000,
    maxHttpBufferSize: 1e6,
    allowEIO3: true
});

// ============================================
// ⚙️ الحصول على الإعدادات مع كاش
// ============================================
let configCache = null;
let configCacheTime = 0;
const CONFIG_CACHE_TTL = 10000;

async function getServerConfig(forceRefresh = false) {
    if (!forceRefresh && configCache && (Date.now() - configCacheTime) < CONFIG_CACHE_TTL) {
        return configCache;
    }
    
    try {
        const result = await pool.query(
            "SELECT value FROM server_config WHERE key = 'server_config'"
        );
        configCache = result.rows[0]?.value || {};
        configCacheTime = Date.now();
        return configCache;
    } catch (error) {
        console.error('❌ Error getting config:', error);
        return configCache || {};
    }
}

async function updateServerConfig(newConfig) {
    try {
        await pool.query(
            `INSERT INTO server_config (key, value) 
             VALUES ('server_config', $1) 
             ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = CURRENT_TIMESTAMP`,
            [newConfig]
        );
        configCache = newConfig;
        configCacheTime = Date.now();
        return true;
    } catch (error) {
        console.error('❌ Error updating config:', error);
        return false;
    }
}

// ============================================
// 🎯 نظام ELO
// ============================================
class ELO {
    static calculate(winnerELO, loserELO) {
        const K = 32;
        const expected = 1 / (1 + Math.pow(10, (loserELO - winnerELO) / 400));
        const change = Math.round(K * (1 - expected));
        return { winnerChange: change, loserChange: -change };
    }

    static getRank(elo) {
        if (elo < 1000) return { rank: 'برونزي', color: '#cd7f32', icon: '🥉' };
        if (elo < 1200) return { rank: 'فضي', color: '#c0c0c0', icon: '🥈' };
        if (elo < 1400) return { rank: 'ذهبي', color: '#ffd700', icon: '🥇' };
        if (elo < 1600) return { rank: 'بلاتيني', color: '#e5e4e2', icon: '💎' };
        if (elo < 1800) return { rank: 'ماسي', color: '#b9f2ff', icon: '👑' };
        if (elo < 2000) return { rank: 'أسطوري', color: '#ff6b6b', icon: '⚡' };
        return { rank: 'خارق', color: '#ff00ff', icon: '🌟' };
    }
}

// ============================================
// 🎮 محرك الفيزياء (Battle Royale)
// ============================================
class GamePhysics {
    constructor(roomId, room) {
        this.roomId = roomId;
        this.room = room;
        this.bullets = [];
        this.tanks = new Map();
        this.obstacles = this.generateObstacles();
        this.tickInterval = null;
        this.lastTick = Date.now();
        this.gameStartTime = Date.now();
        this.killFeed = [];
        this.aliveCount = 0;
        this.gameEnded = false;
        this.killRewards = new Map();
        this.eliminatedPlayers = new Set();
        this.playerStats = new Map();
        this.lastState = null;
        this.gameRound = (room.gameRound || 0) + 1;
        this.pingInterval = null;
        this.lastPing = Date.now();
    }

    generateObstacles() {
        const obstacles = [];
        const positions = [
            { x: -90, z: -70, scale: 3.5 }, { x: -70, z: 40, scale: 4.0 },
            { x: -40, z: -95, scale: 3.2 }, { x: 20, z: -85, scale: 3.8 },
            { x: 85, z: -50, scale: 4.2 }, { x: 95, z: 20, scale: 3.5 },
            { x: 75, z: 85, scale: 4.0 }, { x: -55, z: -95, scale: 3.6 },
            { x: -95, z: 25, scale: 4.5 }, { x: -20, z: 95, scale: 3.3 },
            { x: 50, z: -105, scale: 3.8 }, { x: -85, z: -80, scale: 4.0 },
            { x: -70, z: 85, scale: 3.5 }, { x: 40, z: 105, scale: 3.2 },
            { x: 105, z: 70, scale: 4.0 }, { x: -105, z: -55, scale: 4.2 },
            { x: -50, z: 105, scale: 3.5 }, { x: 90, z: -90, scale: 3.8 },
            { x: 0, z: -110, scale: 4.0 }, { x: -110, z: 0, scale: 4.5 },
            { x: 110, z: -30, scale: 3.8 }, { x: 30, z: 110, scale: 3.5 },
            { x: -95, z: 85, scale: 4.0 }, { x: 95, z: 95, scale: 4.2 }
        ];
        
        positions.forEach(pos => {
            obstacles.push({
                id: `obs_${Math.random().toString(36).substr(2, 6)}`,
                position: { x: pos.x, y: 0, z: pos.z },
                scale: pos.scale,
                radius: pos.scale * 0.8
            });
        });
        return obstacles;
    }

    pointInCircle(point, circle) {
        const dx = point.x - circle.position.x;
        const dz = point.z - circle.position.z;
        return (dx * dx + dz * dz) < (circle.radius * circle.radius);
    }

    checkCollisions(bullet) {
        for (const obstacle of this.obstacles) {
            if (this.pointInCircle(bullet.position, obstacle)) {
                return { hit: true, target: null, type: 'obstacle' };
            }
        }

        for (const [playerId, tank] of this.tanks) {
            if (playerId === bullet.ownerId) continue;
            if (tank.health <= 0) continue;
            
            const tankCircle = {
                position: tank.position,
                radius: 1.5
            };
            
            if (this.pointInCircle(bullet.position, tankCircle)) {
                return { hit: true, target: playerId, type: 'player' };
            }
        }

        const config = this.room?.config || {};
        const boundary = config.boundaryLimit || 280;
        if (Math.abs(bullet.position.x) > boundary || Math.abs(bullet.position.z) > boundary) {
            return { hit: true, target: null, type: 'boundary' };
        }

        return { hit: false };
    }

    fireBullet(ownerId, position, direction) {
        const config = this.room?.config || {};
        const bulletSpeed = config.bulletSpeed || 2.6;
        
        const bullet = {
            id: `bullet_${Date.now()}_${Math.random().toString(36).substr(2, 6)}`,
            ownerId: ownerId,
            position: { ...position },
            velocity: {
                x: direction.x * bulletSpeed,
                y: direction.y * bulletSpeed,
                z: direction.z * bulletSpeed
            },
            life: 200,
            createdAt: Date.now()
        };
        this.bullets.push(bullet);
        return bullet;
    }

    update(timeStep) {
        if (this.gameEnded) return;

        const toRemove = [];
        for (let i = 0; i < this.bullets.length; i++) {
            const bullet = this.bullets[i];
            
            bullet.position.x += bullet.velocity.x * timeStep;
            bullet.position.y += bullet.velocity.y * timeStep;
            bullet.position.z += bullet.velocity.z * timeStep;
            bullet.life--;

            const collision = this.checkCollisions(bullet);
            if (collision.hit || bullet.life <= 0) {
                if (collision.type === 'player' && collision.target) {
                    this.handleDamage(bullet.ownerId, collision.target, 100);
                }
                toRemove.push(i);
            }
        }

        for (const idx of toRemove.sort((a, b) => b - a)) {
            this.bullets.splice(idx, 1);
        }

        for (const [playerId, tank] of this.tanks) {
            if (tank.health <= 0 && !tank.respawning && !this.eliminatedPlayers.has(playerId)) {
                this.handleDeath(playerId);
            }
        }

        this.aliveCount = 0;
        for (const [playerId, tank] of this.tanks) {
            if (tank.health > 0) {
                this.aliveCount++;
            }
        }

        io.to(this.roomId).emit('alive_count_update', { 
            alive: this.aliveCount,
            total: this.tanks.size
        });

        if (this.aliveCount <= 1 && this.tanks.size >= 2) {
            this.endGame();
        }
    }

    async handleDamage(shooterId, targetId, damage) {
        const target = this.tanks.get(targetId);
        if (!target || target.health <= 0 || this.eliminatedPlayers.has(targetId)) return;

        const newHealth = Math.max(0, target.health - damage);
        target.health = newHealth;

        if (newHealth <= 0) {
            const killReward = this.calculateKillReward(shooterId);
            this.killRewards.set(shooterId, (this.killRewards.get(shooterId) || 0) + 1);
            
            const shooter = this.tanks.get(shooterId);
            const targetPlayer = this.room.players.find(p => p.userId === targetId);
            
            const visualSettings = visualSettingsCache.get('kill') || {};
            
            io.to(this.roomId).emit('player_eliminated', {
                userId: targetId,
                killerId: shooterId,
                killReward: killReward,
                position: target.position,
                aliveCount: this.aliveCount - 1,
                imageUrl: visualSettings.imageUrl || '/images/default/kill.png',
                altText: visualSettings.altText || 'أقصيت لاعباً'
            });

            this.killFeed.push({
                killer: shooter?.name || 'لاعب',
                target: targetPlayer?.username || targetPlayer?.first_name || 'لاعب',
                timestamp: Date.now()
            });

            io.to(this.roomId).emit('kill_feed_update', {
                kills: this.killFeed.slice(-10)
            });

            await this.updateKillerBalance(shooterId, killReward);
        }

        io.to(this.roomId).emit('health_update', {
            userId: targetId,
            health: newHealth,
            damage: damage,
            shooterId: shooterId
        });
    }

    calculateKillReward(killerId) {
        const killer = this.room.players.find(p => p.userId === killerId);
        if (!killer) return 0;
        
        const seatPrice = this.room.seatPrice || 1;
        const percent = 0.8;
        return Math.round(seatPrice * percent * 100) / 100;
    }

    async updateKillerBalance(shooterId, reward) {
        try {
            const result = await pool.query(
                'SELECT balance FROM users WHERE id = $1',
                [shooterId]
            );
            const currentBalance = result.rows[0]?.balance || 0;
            
            await pool.query(
                `UPDATE users SET 
                 balance = balance + $1,
                 kills = kills + 1
                 WHERE id = $2`,
                [reward, shooterId]
            );

            const killerSocket = io.sockets.sockets.get(
                this.room.players.find(p => p.userId === shooterId)?.socketId
            );
            if (killerSocket) {
                const visualSettings = visualSettingsCache.get('kill') || {};
                killerSocket.emit('balance_update', {
                    balance: currentBalance + reward,
                    reward: reward,
                    reason: '💀 مكافأة إقصاء',
                    imageUrl: visualSettings.imageUrl || '/images/default/kill.png',
                    altText: visualSettings.altText || 'أقصيت لاعباً'
                });
            }
        } catch (error) {
            console.error('Error updating killer balance:', error);
        }
    }

    async handleDeath(playerId) {
        const tank = this.tanks.get(playerId);
        if (!tank || this.eliminatedPlayers.has(playerId)) return;

        this.eliminatedPlayers.add(playerId);
        tank.health = 0;
        tank.respawning = true;

        const player = this.room.players.find(p => p.userId === playerId);
        if (player) {
            const visualSettings = visualSettingsCache.get('elimination') || {};
            io.to(player.socketId).emit('you_were_eliminated', {
                message: '💀 لقد تم تدمير دبابتك!',
                kills: this.killRewards.get(playerId) || 0,
                imageUrl: visualSettings.imageUrl || '/images/default/elimination.png',
                altText: visualSettings.altText || 'تم إقصاؤك'
            });
        }

        setTimeout(() => {
            this.respawnPlayer(playerId);
        }, 5000);
    }

    respawnPlayer(playerId) {
        if (this.eliminatedPlayers.has(playerId)) return;
        
        const tank = this.tanks.get(playerId);
        if (!tank) return;

        const spawnPositions = [
            { x: -120, z: -80 }, { x: 120, z: 80 },
            { x: -100, z: 60 }, { x: 100, z: -60 },
            { x: -50, z: -100 }, { x: 50, z: 100 },
            { x: -80, z: -120 }, { x: 80, z: 120 }
        ];
        
        const pos = spawnPositions[Math.floor(Math.random() * spawnPositions.length)];
        tank.position = { ...pos };
        tank.health = 100;
        tank.respawning = false;

        io.to(this.roomId).emit('player_respawned', {
            userId: playerId,
            position: tank.position,
            health: 100
        });
    }

    async endGame() {
        if (this.gameEnded) return;
        this.gameEnded = true;

        let winner = null;
        for (const [playerId, tank] of this.tanks) {
            if (tank.health > 0) {
                winner = playerId;
                break;
            }
        }

        const winReward = winner ? this.calculateWinReward(winner) : 0;
        await this.distributeRewards(winner, winReward);

        const duration = Math.floor((Date.now() - this.gameStartTime) / 1000);
        
        const visualSettings = visualSettingsCache.get('winner') || {};
        
        const result = {
            winner: winner,
            winReward: winReward,
            duration: duration,
            totalPlayers: this.tanks.size,
            killFeed: this.killFeed.slice(-10),
            kills: Array.from(this.killRewards.entries()).map(([id, count]) => ({
                userId: id,
                kills: count
            })),
            imageUrl: visualSettings.imageUrl || '/images/default/winner.png',
            altText: visualSettings.altText || 'فائز',
            gameRound: this.gameRound
        };

        io.to(this.roomId).emit('game_ended', result);

        this.room.status = 'ended';
        this.room.gameRound = this.gameRound;
        activeGames.delete(this.roomId);

        try {
            await pool.query(
                `INSERT INTO matches (id, room_id, winner_id, players, kill_feed, start_time, end_time, duration, total_players, game_round)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
                [
                    `match_${Date.now()}_${Math.random().toString(36).substr(2, 6)}`,
                    this.roomId,
                    winner,
                    JSON.stringify(this.room.players.map(p => ({
                        userId: p.userId,
                        username: p.username || p.first_name || 'لاعب',
                        kills: this.killRewards.get(p.userId) || 0
                    }))),
                    JSON.stringify(this.killFeed),
                    new Date(this.gameStartTime),
                    new Date(),
                    duration,
                    this.tanks.size,
                    this.gameRound
                ]
            );
        } catch (error) {
            console.error('Error saving match:', error);
        }

        setTimeout(() => {
            resetRoom(this.roomId);
        }, 10000);

        console.log(`🏆 Game ended in ${this.room.name}. Winner: ${winner}, Round: ${this.gameRound}`);
        monitoring.recordGameEnded();
    }

    calculateWinReward(winnerId) {
        const winner = this.room.players.find(p => p.userId === winnerId);
        if (!winner) return 0;
        
        const seatPrice = this.room.seatPrice || 1;
        const multiplier = 1.5;
        const kills = this.killRewards.get(winnerId) || 0;
        const killBonus = kills * seatPrice * 0.8;
        
        return Math.round((seatPrice * multiplier + killBonus) * 100) / 100;
    }

    async distributeRewards(winnerId, winReward) {
        for (const player of this.room.players) {
            try {
                const result = await pool.query(
                    'SELECT balance, elo FROM users WHERE id = $1',
                    [player.userId]
                );
                const userData = result.rows[0];
                const currentBalance = userData?.balance || 0;
                const currentELO = userData?.elo || 1000;

                let reward = 0;
                let eloChange = 0;

                if (player.userId === winnerId) {
                    reward = winReward;
                    eloChange = 15;
                } else {
                    eloChange = -5;
                    reward = 0;
                }

                const newELO = Math.max(1, currentELO + eloChange);

                await pool.query(
                    `UPDATE users SET 
                     balance = balance + $1,
                     elo = $2,
                     games_played = games_played + 1,
                     wins = wins + $3,
                     total_rewards = total_rewards + $4,
                     last_game = CURRENT_TIMESTAMP
                     WHERE id = $5`,
                    [reward, newELO, player.userId === winnerId ? 1 : 0, reward, player.userId]
                );

                const playerSocket = io.sockets.sockets.get(player.socketId);
                if (playerSocket) {
                    const rank = ELO.getRank(newELO);
                    const visualSettings = visualSettingsCache.get(
                        player.userId === winnerId ? 'winner' : 'game_end'
                    ) || {};
                    
                    playerSocket.emit('balance_update', {
                        balance: currentBalance + reward,
                        reward: reward,
                        elo: newELO,
                        rank: rank,
                        kills: this.killRewards.get(player.userId) || 0,
                        isWinner: player.userId === winnerId,
                        imageUrl: visualSettings.imageUrl || '/images/default/game_end.png',
                        altText: visualSettings.altText || 'انتهت المعركة'
                    });
                }
            } catch (error) {
                console.error('Error distributing rewards:', error);
            }
        }
    }

    start() {
        this.tickInterval = setInterval(() => {
            const now = Date.now();
            const timeStep = (now - this.lastTick) / 1000;
            this.lastTick = now;
            this.update(Math.min(timeStep, 0.05));
        }, 50);
        monitoring.recordGameStarted();
        
        this.pingInterval = setInterval(() => {
            io.to(this.roomId).emit('game_ping', { time: Date.now() });
        }, 5000);
    }

    stop() {
        if (this.tickInterval) {
            clearInterval(this.tickInterval);
            this.tickInterval = null;
        }
        if (this.pingInterval) {
            clearInterval(this.pingInterval);
            this.pingInterval = null;
        }
    }
}

// ============================================
// 🏠 وظائف إدارة الغرف مع قفل
// ============================================
function broadcastRoomsList() {
    const roomsList = [];
    for (const [roomId, room] of rooms) {
        if (room.status === 'waiting' || room.status === 'active') {
            roomsList.push({
                id: roomId,
                name: room.name,
                type: room.type,
                players: room.players.length,
                maxSeats: room.maxSeats,
                seatPrice: room.seatPrice,
                status: room.status,
                needed: room.maxSeats - room.players.length,
                startTime: room.startTime,
                gameRound: room.gameRound || 0
            });
        }
    }
    io.emit('rooms_list', { rooms: roomsList });
}

function broadcastLobbyInfo() {
    const totalPlayers = players.size;
    const activeRooms = Array.from(rooms.values()).filter(r => r.status === 'active').length;
    const waitingRooms = Array.from(rooms.values()).filter(r => r.status === 'waiting').length;
    const totalGames = activeGames.size;
    
    io.emit('lobby_stats', {
        totalPlayers,
        activeRooms,
        waitingRooms,
        totalRooms: rooms.size,
        totalGames,
        serverTime: Date.now()
    });
}

async function resetRoom(roomId) {
    const lockKey = `room_reset_${roomId}`;
    try {
        await lockSystem.acquireLock(lockKey, 'system', 10000);
        
        const oldRoom = rooms.get(roomId);
        if (!oldRoom) {
            lockSystem.releaseLock(lockKey, 'system');
            return;
        }
        
        if (oldRoom.gameInterval) {
            clearInterval(oldRoom.gameInterval);
            oldRoom.gameInterval = null;
        }
        
        if (activeGames.has(roomId)) {
            const game = activeGames.get(roomId);
            game.stop();
            activeGames.delete(roomId);
        }
        
        rooms.delete(roomId);
        
        const config = await getServerConfig();
        const roomConfigs = config.rooms || {};
        const typeConfig = roomConfigs[oldRoom.type];
        
        if (typeConfig && typeConfig.enabled) {
            const typeNames = {
                beginner: 'غرفة المبتدئين',
                advanced: 'غرفة المتقدمين',
                pro: 'غرفة المحترفين'
            };
            
            let roomNumber = 1;
            let newRoomId = `${oldRoom.type}_room_${roomNumber}`;
            while (rooms.has(newRoomId)) {
                roomNumber++;
                newRoomId = `${oldRoom.type}_room_${roomNumber}`;
            }
            
            const newRoom = {
                id: newRoomId,
                type: oldRoom.type,
                name: `${typeNames[oldRoom.type] || oldRoom.type} ${roomNumber}`,
                maxSeats: typeConfig.maxSeats,
                seatPrice: typeConfig.seatPrice,
                rewardMultiplier: typeConfig.rewardMultiplier || 1,
                players: [],
                status: 'waiting',
                createdAt: Date.now(),
                startTime: null,
                roomNumber: roomNumber,
                gameRound: (oldRoom.gameRound || 0) + 1
            };
            
            rooms.set(newRoomId, newRoom);
            
            await pool.query(
                `INSERT INTO rooms (id, type, name, max_seats, seat_price, reward_multiplier, status, players, game_round)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                 ON CONFLICT (id) DO UPDATE SET
                 type = EXCLUDED.type,
                 name = EXCLUDED.name,
                 max_seats = EXCLUDED.max_seats,
                 seat_price = EXCLUDED.seat_price,
                 reward_multiplier = EXCLUDED.reward_multiplier,
                 status = EXCLUDED.status,
                 game_round = EXCLUDED.game_round`,
                [newRoomId, newRoom.type, newRoom.name, newRoom.maxSeats, newRoom.seatPrice, newRoom.rewardMultiplier, 'waiting', '[]', newRoom.gameRound]
            );
            
            console.log(`🔄 Replaced ${oldRoom.name} with ${newRoom.name} (Round ${newRoom.gameRound})`);
            
            io.emit('room_created', {
                roomId: newRoomId,
                name: newRoom.name,
                maxSeats: newRoom.maxSeats,
                seatPrice: newRoom.seatPrice,
                gameRound: newRoom.gameRound
            });
        }
        
        broadcastRoomsList();
        broadcastLobbyInfo();
        
        lockSystem.releaseLock(lockKey, 'system');
    } catch (error) {
        console.error('Error resetting room:', error);
        lockSystem.releaseLock(lockKey, 'system');
    }
}

async function updateRoom(roomId) {
    const lockKey = `room_update_${roomId}`;
    try {
        await lockSystem.acquireLock(lockKey, 'system', 5000);
        
        const room = rooms.get(roomId);
        if (!room) {
            lockSystem.releaseLock(lockKey, 'system');
            return;
        }
        
        io.to(roomId).emit('room_update', {
            players: room.players.map(p => ({ 
                userId: p.userId, 
                username: p.username || p.first_name || 'لاعب',
                elo: p.elo || 1000
            })),
            maxSeats: room.maxSeats,
            count: room.players.length,
            seatPrice: room.seatPrice,
            needed: room.maxSeats - room.players.length
        });
        
        if (room.players.length >= Math.min(room.maxSeats, 2) && 
            room.status === 'waiting' &&
            room.players.length >= 2) {
            startGame(roomId);
        }
        
        lockSystem.releaseLock(lockKey, 'system');
    } catch (error) {
        console.error('Error updating room:', error);
        lockSystem.releaseLock(lockKey, 'system');
    }
}

async function startGame(roomId) {
    const lockKey = `game_start_${roomId}`;
    try {
        await lockSystem.acquireLock(lockKey, 'system', 10000);
        
        const room = rooms.get(roomId);
        if (!room || room.status !== 'waiting') {
            lockSystem.releaseLock(lockKey, 'system');
            return;
        }
        
        room.status = 'active';
        room.startTime = Date.now();
        
        const players = room.players;
        
        const spawnPositions = [
            { x: -120, z: -80 }, { x: 120, z: 80 },
            { x: -100, z: 60 }, { x: 100, z: -60 },
            { x: -50, z: -100 }, { x: 50, z: 100 },
            { x: -80, z: -120 }, { x: 80, z: 120 },
            { x: 0, z: -100 }, { x: 0, z: 100 },
            { x: -100, z: 0 }, { x: 100, z: 0 }
        ];
        
        const shuffled = [...spawnPositions].sort(() => Math.random() - 0.5);
        
        for (let i = 0; i < players.length; i++) {
            const pos = shuffled[i % shuffled.length];
            players[i].position = { x: pos.x, z: pos.z, y: 0 };
            players[i].rotation = 0;
            players[i].health = 100;
            players[i].kills = 0;
            players[i].team = null;
        }
        
        const visualSettings = visualSettingsCache.get('game_start') || {};
        
        const game = new GamePhysics(roomId, room);
        
        for (const player of players) {
            game.tanks.set(player.userId, {
                position: { ...player.position },
                health: 100,
                respawning: false,
                name: player.username || player.first_name || 'لاعب'
            });
        }
        
        game.start();
        activeGames.set(roomId, game);
        
        for (const player of players) {
            io.to(player.socketId).emit('game_start', {
                roomId: roomId,
                players: players.map(p => ({ 
                    userId: p.userId, 
                    position: p.position,
                    health: p.health
                })),
                yourId: player.userId,
                startTime: room.startTime,
                position: player.position,
                health: 100,
                gameId: `match_${Date.now()}_${Math.random().toString(36).substr(2, 6)}`,
                mode: 'battle_royale',
                totalPlayers: players.length,
                imageUrl: visualSettings.imageUrl || '/images/default/game_start.png',
                altText: visualSettings.altText || 'بدء المعركة',
                gameRound: room.gameRound || 1
            });
        }
        
        console.log(`🎮 Battle Royale started in ${room.name} with ${players.length} players (Round ${room.gameRound || 1})`);
        broadcastRoomsList();
        broadcastLobbyInfo();
        
        lockSystem.releaseLock(lockKey, 'system');
    } catch (error) {
        console.error('Error starting game:', error);
        lockSystem.releaseLock(lockKey, 'system');
    }
}

// ============================================
// 👤 دوال المستخدمين
// ============================================
async function getUserData(userId) {
    try {
        // التحقق من الكاش
        if (userCache.has(userId)) {
            const cached = userCache.get(userId);
            if (Date.now() - cached.timestamp < 30000) {
                return cached.data;
            }
        }
        
        const result = await pool.query('SELECT * FROM users WHERE id = $1', [userId]);
        if (result.rows.length === 0) {
            // إنشاء مستخدم جديد
            const newUser = {
                id: userId,
                telegram_id: userId,
                username: `لاعب_${userId.slice(0, 6)}`,
                balance: 0,
                elo: 1000,
                kills: 0,
                wins: 0,
                games_played: 0,
                is_admin: userId === '7011476249' // الآدمن المحدد
            };
            
            await pool.query(
                `INSERT INTO users (id, telegram_id, username, balance, elo, is_admin, created_at, last_login)
                 VALUES ($1, $2, $3, $4, $5, $6, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
                [newUser.id, newUser.telegram_id, newUser.username, newUser.balance, newUser.elo, newUser.is_admin]
            );
            
            userCache.set(userId, { data: newUser, timestamp: Date.now() });
            return newUser;
        }
        
        const user = result.rows[0];
        userCache.set(userId, { data: user, timestamp: Date.now() });
        return user;
        
    } catch (error) {
        console.error('Error getting user data:', error);
        throw error;
    }
}

// ============================================
// 🔌 أحداث Socket.io
// ============================================
io.on('connection', (socket) => {
    console.log(`🔌 New connection: ${socket.id}`);
    monitoring.recordConnection('connect');
    
    const playerData = {
        socketId: socket.id,
        userId: null,
        username: null,
        roomId: null,
        isAdmin: false,
        balance: 0,
        elo: 1000,
        connectedAt: Date.now(),
        ipAddress: socket.handshake.address,
        deviceId: socket.handshake.query.deviceId || null,
        lastPing: Date.now()
    };
    
    players.set(socket.id, playerData);
    
    // ============================================
    // 🔐 المصادقة عبر Telegram ID من الرابط
    // ============================================
    socket.on('auth', async (data) => {
        const startTime = Date.now();
        const authTimeout = setTimeout(() => {
            socket.emit('auth_error', { message: 'Authentication timeout' });
            monitoring.recordRequest(false, Date.now() - startTime);
        }, 15000);
        
        try {
            const { telegramId } = data;
            
            if (!telegramId) {
                clearTimeout(authTimeout);
                socket.emit('auth_error', { message: 'Telegram ID required' });
                monitoring.recordRequest(false, Date.now() - startTime);
                return;
            }
            
            if (!antiCheat.checkRateLimit(telegramId, 'auth')) {
                clearTimeout(authTimeout);
                socket.emit('auth_error', { message: 'Too many authentication attempts' });
                monitoring.recordRequest(false, Date.now() - startTime);
                return;
            }
            
            const config = await getServerConfig();
            if (config.system?.maintenanceMode) {
                clearTimeout(authTimeout);
                socket.emit('auth_error', { 
                    message: '🔧 الخادم في وضع الصيانة. يرجى المحاولة لاحقاً.' 
                });
                monitoring.recordRequest(false, Date.now() - startTime);
                return;
            }
            
            if (await antiCheat.isUserBanned(telegramId)) {
                clearTimeout(authTimeout);
                socket.emit('auth_error', { 
                    message: '🚫 تم حظر حسابك بسبب انتهاك قواعد اللعبة.' 
                });
                monitoring.recordRequest(false, Date.now() - startTime);
                return;
            }
            
            // جلب بيانات المستخدم
            const userData = await getUserData(telegramId);
            
            const player = players.get(socket.id);
            if (player) {
                player.userId = userData.id;
                player.username = userData.username || `لاعب_${userData.id.slice(0, 6)}`;
                player.isAdmin = userData.is_admin || false;
                player.balance = userData.balance || 0;
                player.elo = userData.elo || 1000;
            }
            
            clearTimeout(authTimeout);
            
            const rank = ELO.getRank(userData.elo || 1000);
            
            // إرسال بيانات المستخدم
            socket.emit('auth_success', {
                userId: userData.id,
                telegramId: userData.telegram_id,
                username: userData.username || `لاعب_${userData.id.slice(0, 6)}`,
                balance: userData.balance || 0,
                elo: userData.elo || 1000,
                rank: rank,
                kills: userData.kills || 0,
                wins: userData.wins || 0,
                gamesPlayed: userData.games_played || 0,
                isAdmin: userData.is_admin || false,
                timestamp: Date.now()
            });
            
            // تحديث آخر تسجيل دخول
            await pool.query(
                'UPDATE users SET last_login = CURRENT_TIMESTAMP, login_count = login_count + 1 WHERE id = $1',
                [userData.id]
            );
            
            console.log(`✅ Auth: ${userData.id} (${userData.username})`);
            monitoring.recordRequest(true, Date.now() - startTime);
            
            // إرسال الإعدادات البصرية
            const visualSettings = {};
            for (const [key, value] of visualSettingsCache) {
                visualSettings[key] = value;
            }
            socket.emit('visual_settings', visualSettings);
            
            // إرسال إعدادات الخادم
            socket.emit('server_config', config);
            
        } catch (error) {
            clearTimeout(authTimeout);
            console.error('❌ Auth error:', error);
            socket.emit('auth_error', { message: 'Authentication failed: ' + error.message });
            monitoring.recordRequest(false, Date.now() - startTime);
            monitoring.recordError('auth_error', error.message);
        }
    });
    
    // ============================================
    // 🏠 اللوبي
    // ============================================
    socket.on('join_lobby', async () => {
        const startTime = Date.now();
        const player = players.get(socket.id);
        if (!player?.userId) {
            socket.emit('error', { message: 'Not authenticated' });
            monitoring.recordRequest(false, Date.now() - startTime);
            return;
        }
        
        try {
            const userData = await getUserData(player.userId);
            player.balance = userData.balance || 0;
            player.elo = userData.elo || 1000;
            
            const config = await getServerConfig();
            
            const visualSettings = {};
            for (const [key, value] of visualSettingsCache) {
                visualSettings[key] = value;
            }
            
            socket.emit('lobby_joined', {
                balance: userData.balance || 0,
                elo: userData.elo || 1000,
                rank: ELO.getRank(userData.elo || 1000),
                userId: player.userId,
                username: userData.username || `لاعب_${player.userId.slice(0, 6)}`,
                isAdmin: userData.is_admin || false,
                config: {
                    rooms: config.rooms || {},
                    game: config.game || {},
                    system: config.system || {},
                    appearance: config.appearance || {}
                },
                visualSettings: visualSettings,
                serverTime: Date.now()
            });
            
            broadcastRoomsList();
            broadcastLobbyInfo();
            
            console.log(`🏠 ${player.userId} joined lobby`);
            monitoring.recordRequest(true, Date.now() - startTime);
            
        } catch (error) {
            console.error('Error joining lobby:', error);
            socket.emit('error', { message: 'Could not join lobby' });
            monitoring.recordRequest(false, Date.now() - startTime);
            monitoring.recordError('lobby_error', error.message);
        }
    });
    
    // ============================================
    // 🏠 الغرف
    // ============================================
    socket.on('list_rooms', () => {
        broadcastRoomsList();
    });
    
    socket.on('join_room', async (data) => {
        const startTime = Date.now();
        const player = players.get(socket.id);
        if (!player?.userId) {
            socket.emit('error', { message: 'Not authenticated' });
            monitoring.recordRequest(false, Date.now() - startTime);
            return;
        }
        
        const { roomId } = data;
        const room = rooms.get(roomId);
        
        if (!room) {
            socket.emit('error', { message: 'Room not found' });
            monitoring.recordRequest(false, Date.now() - startTime);
            return;
        }
        
        if (room.status !== 'waiting') {
            socket.emit('error', { message: 'Game in progress' });
            monitoring.recordRequest(false, Date.now() - startTime);
            return;
        }
        
        if (room.players.length >= room.maxSeats) {
            socket.emit('error', { message: 'Room is full' });
            monitoring.recordRequest(false, Date.now() - startTime);
            return;
        }
        
        if (player.roomId) {
            socket.emit('error', { message: 'You are already in a room' });
            monitoring.recordRequest(false, Date.now() - startTime);
            return;
        }
        
        if (!antiCheat.checkRateLimit(player.userId, 'join')) {
            socket.emit('error', { message: 'Too many join attempts' });
            monitoring.recordRequest(false, Date.now() - startTime);
            return;
        }
        
        const lockKey = `room_join_${roomId}`;
        try {
            await lockSystem.acquireLock(lockKey, player.userId, 5000);
            
            // التحقق مرة أخرى بعد القفل
            if (room.status !== 'waiting') {
                socket.emit('error', { message: 'Game already started' });
                lockSystem.releaseLock(lockKey, player.userId);
                monitoring.recordRequest(false, Date.now() - startTime);
                return;
            }
            
            if (room.players.length >= room.maxSeats) {
                socket.emit('error', { message: 'Room is full' });
                lockSystem.releaseLock(lockKey, player.userId);
                monitoring.recordRequest(false, Date.now() - startTime);
                return;
            }
            
            const userData = await getUserData(player.userId);
            const balance = userData.balance || 0;
            const seatPrice = room.seatPrice || 1;
            
            if (balance < seatPrice) {
                socket.emit('error', { message: `⚠️ رصيدك غير كافٍ! سعر المقعد: ${seatPrice}$` });
                lockSystem.releaseLock(lockKey, player.userId);
                monitoring.recordRequest(false, Date.now() - startTime);
                return;
            }
            
            // خصم الرصيد
            await pool.query(
                'UPDATE users SET balance = balance - $1 WHERE id = $2',
                [seatPrice, player.userId]
            );
            
            const newBalance = balance - seatPrice;
            player.balance = newBalance;
            
            const newPlayer = {
                userId: player.userId,
                socketId: socket.id,
                username: userData.username || `لاعب_${player.userId.slice(0, 6)}`,
                balance: newBalance,
                health: 100,
                paidAmount: seatPrice,
                elo: userData.elo || 1000,
                kills: 0,
                joinedAt: Date.now()
            };
            
            room.players.push(newPlayer);
            player.roomId = room.id;
            socket.join(room.id);
            
            await pool.query(
                `UPDATE rooms SET players = $1 WHERE id = $2`,
                [JSON.stringify(room.players), room.id]
            );
            
            // إرسال تحديث المقاعد
            socket.emit('room_joined', {
                roomId: roomId,
                balance: newBalance,
                roomName: room.name,
                playersCount: room.players.length,
                maxSeats: room.maxSeats,
                seatPrice: room.seatPrice,
                players: room.players.map(p => ({
                    userId: p.userId,
                    username: p.username || 'لاعب',
                    isYou: p.userId === player.userId
                })),
                needed: room.maxSeats - room.players.length,
                gameRound: room.gameRound || 1,
                message: `✅ تم الانضمام إلى ${room.name}\n💰 تم خصم ${seatPrice}$\n👥 ${room.players.length}/${room.maxSeats} لاعب`
            });
            
            io.to(room.id).emit('player_joined', {
                userId: player.userId,
                username: userData.username || `لاعب_${player.userId.slice(0, 6)}`,
                playersCount: room.players.length,
                maxSeats: room.maxSeats,
                elo: userData.elo || 1000
            });
            
            lockSystem.releaseLock(lockKey, player.userId);
            updateRoom(room.id);
            broadcastRoomsList();
            
            console.log(`👥 ${player.userId} joined ${room.name} (${room.players.length}/${room.maxSeats})`);
            monitoring.recordRequest(true, Date.now() - startTime);
            
        } catch (error) {
            console.error('Error joining room:', error);
            socket.emit('error', { message: 'Could not join room: ' + error.message });
            lockSystem.releaseLock(lockKey, player.userId);
            monitoring.recordRequest(false, Date.now() - startTime);
            monitoring.recordError('join_room_error', error.message);
        }
    });
    
    // مغادرة الغرفة
    socket.on('leave_room', async () => {
        const startTime = Date.now();
        const player = players.get(socket.id);
        if (!player || !player.roomId) {
            socket.emit('error', { message: 'You are not in any room' });
            monitoring.recordRequest(false, Date.now() - startTime);
            return;
        }
        
        const room = rooms.get(player.roomId);
        if (!room) {
            player.roomId = null;
            monitoring.recordRequest(true, Date.now() - startTime);
            return;
        }
        
        if (room.status !== 'waiting') {
            socket.emit('error', { message: 'Cannot leave during battle' });
            monitoring.recordRequest(false, Date.now() - startTime);
            return;
        }
        
        const lockKey = `room_leave_${room.id}`;
        try {
            await lockSystem.acquireLock(lockKey, player.userId, 5000);
            
            const index = room.players.findIndex(p => p.socketId === socket.id);
            if (index !== -1) {
                const removed = room.players[index];
                const refund = removed.paidAmount || room.seatPrice;
                
                // إعادة الرصيد
                await pool.query(
                    'UPDATE users SET balance = balance + $1 WHERE id = $2',
                    [refund, player.userId]
                );
                
                socket.emit('room_left', {
                    roomName: room.name,
                    refunded: refund,
                    message: `🚪 تم مغادرة ${room.name}\n💰 تم إعادة ${refund}$`
                });
                
                room.players.splice(index, 1);
                socket.leave(room.id);
                player.roomId = null;
                
                await pool.query(
                    `UPDATE rooms SET players = $1 WHERE id = $2`,
                    [JSON.stringify(room.players), room.id]
                );
                
                io.to(room.id).emit('player_left', {
                    userId: player.userId,
                    playersCount: room.players.length,
                    maxSeats: room.maxSeats
                });
                
                lockSystem.releaseLock(lockKey, player.userId);
                updateRoom(room.id);
                broadcastRoomsList();
            }
            
            monitoring.recordRequest(true, Date.now() - startTime);
        } catch (error) {
            console.error('Error leaving room:', error);
            socket.emit('error', { message: 'Could not leave room' });
            lockSystem.releaseLock(lockKey, player.userId);
            monitoring.recordRequest(false, Date.now() - startTime);
            monitoring.recordError('leave_room_error', error.message);
        }
    });
    
    // ============================================
    // 🎮 أحداث اللعبة
    // ============================================
    socket.on('move', (data) => {
        const player = players.get(socket.id);
        if (!player?.roomId) return;
        
        if (!antiCheat.checkRateLimit(player.userId, 'move')) {
            return;
        }
        
        const room = rooms.get(player.roomId);
        if (!room || room.status !== 'active') return;
        
        const game = activeGames.get(player.roomId);
        if (!game) return;
        
        const tank = game.tanks.get(player.userId);
        if (!tank || tank.health <= 0) return;
        
        tank.position = { ...data.position };
        tank.rotation = data.rotation || tank.rotation;
        
        socket.to(player.roomId).emit('player_moved', {
            userId: player.userId,
            position: data.position,
            rotation: data.rotation,
            timestamp: Date.now()
        });
    });
    
    socket.on('shoot', (data) => {
        const player = players.get(socket.id);
        if (!player?.roomId) return;
        
        if (!antiCheat.checkRateLimit(player.userId, 'shoot')) {
            return;
        }
        
        const room = rooms.get(player.roomId);
        if (!room || room.status !== 'active') return;
        
        const game = activeGames.get(player.roomId);
        if (!game) return;
        
        const tank = game.tanks.get(player.userId);
        if (!tank || tank.health <= 0) return;
        
        const bullet = game.fireBullet(player.userId, data.position, {
            x: data.direction.x || 0,
            y: data.direction.y || 0,
            z: data.direction.z || 1
        });
        
        io.to(player.roomId).emit('player_shot', {
            userId: player.userId,
            position: data.position,
            direction: data.direction,
            bulletId: bullet.id,
            timestamp: Date.now()
        });
    });
    
    // ============================================
    // 📊 الإحصائيات
    // ============================================
    socket.on('get_stats', async () => {
        const player = players.get(socket.id);
        if (!player?.userId) return;
        
        try {
            const userData = await getUserData(player.userId);
            socket.emit('stats_update', {
                balance: userData.balance || 0,
                elo: userData.elo || 1000,
                rank: ELO.getRank(userData.elo || 1000),
                kills: userData.kills || 0,
                wins: userData.wins || 0,
                gamesPlayed: userData.games_played || 0,
                totalRewards: userData.total_rewards || 0
            });
        } catch (error) {
            console.error('Error getting stats:', error);
        }
    });
    
    socket.on('get_leaderboard', async () => {
        try {
            if (leaderboardCache.size > 0 && Date.now() - leaderboardCache.timestamp < 30000) {
                socket.emit('leaderboard_update', { players: Array.from(leaderboardCache.values()) });
                return;
            }
            
            const result = await pool.query(
                `SELECT id, username, elo, kills, wins, games_played 
                 FROM users 
                 ORDER BY elo DESC 
                 LIMIT 100`
            );
            
            const leaderboard = result.rows.map(row => ({
                userId: row.id,
                username: row.username || 'لاعب',
                elo: row.elo || 1000,
                rank: ELO.getRank(row.elo || 1000),
                kills: row.kills || 0,
                wins: row.wins || 0,
                gamesPlayed: row.games_played || 0
            }));
            
            leaderboardCache.clear();
            leaderboard.forEach(p => leaderboardCache.set(p.userId, p));
            leaderboardCache.timestamp = Date.now();
            
            socket.emit('leaderboard_update', { players: leaderboard });
        } catch (error) {
            console.error('Error getting leaderboard:', error);
        }
    });
    
    // ============================================
    // 🔧 أوامر المدير
    // ============================================
    socket.on('admin_login', async (data) => {
        const player = players.get(socket.id);
        if (!player?.userId) return;
        
        try {
            const userData = await getUserData(player.userId);
            if (!userData.is_admin) {
                socket.emit('admin_error', { message: 'غير مصرح لك بالدخول' });
                return;
            }
            
            const { password } = data;
            if (!antiCheat.verifyAdminPassword(password)) {
                socket.emit('admin_error', { message: 'كلمة مرور خاطئة' });
                monitoring.recordAdminAction('login_failed');
                return;
            }
            
            socket.emit('admin_login_success', { message: 'تم تسجيل الدخول بنجاح' });
            monitoring.recordAdminAction('login');
            
            // إرسال جميع البيانات الإدارية
            await sendAdminData(socket);
            
        } catch (error) {
            console.error('Admin login error:', error);
            socket.emit('admin_error', { message: error.message });
        }
    });
    
    socket.on('admin_command', async (data) => {
        const player = players.get(socket.id);
        if (!player?.userId) return;
        
        try {
            const userData = await getUserData(player.userId);
            if (!userData.is_admin) {
                socket.emit('admin_error', { message: 'Unauthorized' });
                return;
            }
            
            const { command, params } = data;
            const config = await getServerConfig();
            
            switch (command) {
                case 'get_config':
                    socket.emit('admin_config', config);
                    break;
                    
                case 'update_config':
                    const { section, key, value } = params;
                    if (section && key && value !== undefined) {
                        if (section === 'system' && key === 'adminPassword') {
                            // تحديث كلمة المرور
                            antiCheat.adminPassword = value;
                            if (process.env.ADMIN_PASSWORD) {
                                // إذا كانت كلمة المرور من المتغيرات البيئية، لا نغيرها
                                socket.emit('admin_message', {
                                    message: '⚠️ كلمة المرور محددة في المتغيرات البيئية، لا يمكن تغييرها من هنا',
                                    type: 'warning'
                                });
                                break;
                            }
                        }
                        config[section][key] = value;
                        await updateServerConfig(config);
                        socket.emit('admin_message', { 
                            message: `✅ تم تحديث ${section}.${key}`,
                            type: 'success'
                        });
                        if (section === 'rooms') {
                            await initializeRooms();
                        }
                        monitoring.recordAdminAction('update_config');
                    }
                    break;
                    
                case 'reset_config':
                    const defaultConfig = {
                        rooms: {
                            beginner: { enabled: true, maxSeats: 8, seatPrice: 1, rewardMultiplier: 1, maxRooms: 5 },
                            advanced: { enabled: true, maxSeats: 12, seatPrice: 5, rewardMultiplier: 2, maxRooms: 3 },
                            pro: { enabled: true, maxSeats: 16, seatPrice: 10, rewardMultiplier: 3, maxRooms: 2 }
                        },
                        game: {
                            duration: 300000,
                            mapSize: 600,
                            boundaryLimit: 280,
                            bulletSpeed: 2.6,
                            bulletDamage: 100,
                            fireCooldown: 3000,
                            tankHealth: 100,
                            respawnTime: 5000,
                            killRewardPercent: 0.8,
                            winRewardMultiplier: 1.5,
                            minPlayersToStart: 2
                        },
                        system: {
                            maintenanceMode: false,
                            maxPlayersPerMatch: 16,
                            leaderboardCacheTime: 30000,
                            reconnectTimeout: 30000,
                            antiCheatEnabled: true,
                            maxLoginAttempts: 5,
                            lockTimeout: 30000,
                            adminPassword: antiCheat.adminPassword
                        },
                        appearance: {
                            gameLogo: '/images/default/logo.png',
                            backgroundImage: '/images/default/background.jpg'
                        }
                    };
                    await updateServerConfig(defaultConfig);
                    await initializeRooms();
                    socket.emit('admin_message', { 
                        message: '✅ تم إعادة تعيين الإعدادات',
                        type: 'success'
                    });
                    monitoring.recordAdminAction('reset_config');
                    break;
                    
                case 'toggle_maintenance':
                    config.system.maintenanceMode = !config.system.maintenanceMode;
                    await updateServerConfig(config);
                    socket.emit('admin_message', { 
                        message: `🔧 وضع الصيانة ${config.system.maintenanceMode ? 'مفعل' : 'معطل'}`,
                        type: 'info'
                    });
                    broadcastLobbyInfo();
                    monitoring.recordAdminAction('toggle_maintenance');
                    break;
                    
                case 'toggle_anticheat':
                    antiCheat.enabled = !antiCheat.enabled;
                    config.system.antiCheatEnabled = antiCheat.enabled;
                    await updateServerConfig(config);
                    socket.emit('admin_message', { 
                        message: `🛡️ الحماية ${antiCheat.enabled ? 'مفعلة' : 'معطلة'}`,
                        type: 'info'
                    });
                    monitoring.recordAdminAction('toggle_anticheat');
                    break;
                    
                case 'get_stats':
                    const stats = await getAdminStats();
                    socket.emit('admin_stats', { stats });
                    break;
                    
                case 'get_players':
                    const playersList = Array.from(players.values()).map(p => ({
                        userId: p.userId,
                        username: p.username,
                        roomId: p.roomId,
                        balance: p.balance,
                        elo: p.elo,
                        connectedAt: p.connectedAt,
                        ipAddress: p.ipAddress
                    }));
                    socket.emit('admin_players', { players: playersList });
                    break;
                    
                case 'get_rooms':
                    const roomsList = Array.from(rooms.values()).map(r => ({
                        id: r.id,
                        name: r.name,
                        status: r.status,
                        players: r.players.length,
                        maxSeats: r.maxSeats,
                        seatPrice: r.seatPrice,
                        gameRound: r.gameRound || 0,
                        playersList: r.players.map(p => ({
                            userId: p.userId,
                            username: p.username || p.telegramId,
                            health: p.health,
                            kills: p.kills || 0
                        }))
                    }));
                    socket.emit('admin_rooms', { rooms: roomsList });
                    break;
                    
                case 'get_active_games':
                    const games = Array.from(activeGames.entries()).map(([id, game]) => ({
                        roomId: id,
                        players: Array.from(game.tanks.keys()),
                        aliveCount: game.aliveCount,
                        bullets: game.bullets.length,
                        duration: Math.floor((Date.now() - game.gameStartTime) / 1000),
                        gameRound: game.gameRound || 0
                    }));
                    socket.emit('admin_active_games', { games });
                    break;
                    
                case 'force_unlock':
                    const { resourceId, userId } = params;
                    const unlocked = lockSystem.forceUnlock(resourceId, userId);
                    socket.emit('admin_message', { 
                        message: unlocked ? `✅ تم فتح القفل لـ ${resourceId}` : '❌ لم يتم العثور على القفل',
                        type: unlocked ? 'success' : 'error'
                    });
                    monitoring.recordAdminAction('force_unlock');
                    break;
                    
                case 'get_visual_settings':
                    const visualResult = await pool.query('SELECT * FROM visual_settings');
                    socket.emit('admin_visual_settings', visualResult.rows);
                    break;
                    
                case 'update_visual_setting':
                    const { eventKey, imageUrl, altText } = params;
                    await pool.query(
                        `INSERT INTO visual_settings (event_key, image_url, alt_text) 
                         VALUES ($1, $2, $3) 
                         ON CONFLICT (event_key) DO UPDATE SET 
                         image_url = EXCLUDED.image_url,
                         alt_text = EXCLUDED.alt_text,
                         updated_at = CURRENT_TIMESTAMP`,
                        [eventKey, imageUrl, altText]
                    );
                    await loadVisualSettings();
                    socket.emit('admin_message', { 
                        message: `✅ تم تحديث الصورة لـ ${eventKey}`,
                        type: 'success'
                    });
                    monitoring.recordAdminAction('update_visual');
                    break;
                    
                case 'update_appearance':
                    const { logo, background } = params;
                    if (logo) config.appearance.gameLogo = logo;
                    if (background) config.appearance.backgroundImage = background;
                    await updateServerConfig(config);
                    socket.emit('admin_message', {
                        message: '✅ تم تحديث مظهر اللعبة',
                        type: 'success'
                    });
                    // بث التحديث لجميع اللاعبين
                    io.emit('appearance_update', config.appearance);
                    monitoring.recordAdminAction('update_appearance');
                    break;
                    
                case 'kick_player':
                    await kickPlayer(socket, params);
                    monitoring.recordAdminAction('kick_player');
                    break;
                    
                case 'ban_player':
                    await banPlayer(socket, params);
                    monitoring.recordAdminAction('ban_player');
                    break;
                    
                case 'set_balance':
                    await setPlayerBalance(socket, params);
                    monitoring.recordAdminAction('set_balance');
                    break;
                    
                case 'set_admin':
                    const { targetUserId, isAdmin } = params;
                    await pool.query(
                        'UPDATE users SET is_admin = $1 WHERE id = $2',
                        [isAdmin, targetUserId]
                    );
                    // تحديث الكاش
                    userCache.delete(targetUserId);
                    socket.emit('admin_message', {
                        message: `✅ تم ${isAdmin ? 'ترقية' : 'إزالة صلاحيات'} المستخدم ${targetUserId}`,
                        type: 'success'
                    });
                    monitoring.recordAdminAction('set_admin');
                    break;
                    
                case 'get_database_health':
                    socket.emit('admin_message', {
                        message: `Database Health: ${JSON.stringify(db.getHealth())}`,
                        type: 'info'
                    });
                    break;
                    
                case 'force_reconnect_db':
                    socket.emit('admin_message', {
                        message: '🔄 Forcing database reconnection...',
                        type: 'info'
                    });
                    db.isConnected = false;
                    await db.connect();
                    socket.emit('admin_message', {
                        message: db.isConnected ? '✅ Database reconnected successfully' : '❌ Database reconnection failed',
                        type: db.isConnected ? 'success' : 'error'
                    });
                    monitoring.recordAdminAction('force_reconnect');
                    break;
                    
                default:
                    socket.emit('admin_error', { message: 'Unknown command' });
            }
        } catch (error) {
            console.error('Admin command error:', error);
            socket.emit('admin_error', { message: error.message });
            monitoring.recordError('admin_command_error', error.message);
        }
    });
    
    // ============================================
    // 🔌 انقطاع الاتصال
    // ============================================
    socket.on('disconnect', async () => {
        const player = players.get(socket.id);
        if (player) {
            if (player.userId && player.roomId) {
                const reconnectToken = crypto.randomBytes(32).toString('hex');
                pendingReconnects.set(reconnectToken, {
                    userId: player.userId,
                    roomId: player.roomId,
                    socketId: socket.id,
                    timestamp: Date.now()
                });
                
                setTimeout(() => {
                    if (pendingReconnects.has(reconnectToken)) {
                        pendingReconnects.delete(reconnectToken);
                    }
                }, 30000);
            }
            
            if (player.roomId) {
                const room = rooms.get(player.roomId);
                if (room) {
                    const index = room.players.findIndex(p => p.socketId === socket.id);
                    if (index !== -1) {
                        room.players.splice(index, 1);
                        try {
                            await pool.query(
                                `UPDATE rooms SET players = $1 WHERE id = $2`,
                                [JSON.stringify(room.players), room.id]
                            );
                        } catch (error) {
                            console.error('Error updating room on disconnect:', error);
                        }
                    }
                    
                    if (room.status === 'active') {
                        const game = activeGames.get(player.roomId);
                        if (game) {
                            const alivePlayers = Array.from(game.tanks.values()).filter(t => t.health > 0);
                            if (alivePlayers.length <= 1) {
                                game.endGame();
                            }
                        }
                    } else if (room.status === 'waiting') {
                        updateRoom(player.roomId);
                        broadcastRoomsList();
                    }
                }
            }
            
            players.delete(socket.id);
            broadcastRoomsList();
            broadcastLobbyInfo();
            monitoring.recordConnection('disconnect');
        }
        console.log(`🔌 Disconnected: ${socket.id}`);
    });
});

// ============================================
// 🔧 دوال مساعدة للمدير
// ============================================
async function sendAdminData(socket) {
    try {
        // إرسال جميع البيانات الإدارية
        const config = await getServerConfig(true);
        socket.emit('admin_config', config);
        
        const visualResult = await pool.query('SELECT * FROM visual_settings');
        socket.emit('admin_visual_settings', visualResult.rows);
        
        const stats = await getAdminStats();
        socket.emit('admin_stats', { stats });
        
        const monitoringData = {
            monitoring: monitoring.getStats(),
            locks: lockSystem.getStats(),
            antiCheat: antiCheat.getStats(),
            queue: actionProcessor.getStats(),
            database: db.getHealth()
        };
        socket.emit('admin_monitoring', monitoringData);
        
        // قائمة اللاعبين
        const playersList = Array.from(players.values()).map(p => ({
            userId: p.userId,
            username: p.username,
            roomId: p.roomId,
            balance: p.balance,
            elo: p.elo,
            connectedAt: p.connectedAt,
            ipAddress: p.ipAddress
        }));
        socket.emit('admin_players', { players: playersList });
        
        // قائمة الغرف
        const roomsList = Array.from(rooms.values()).map(r => ({
            id: r.id,
            name: r.name,
            status: r.status,
            players: r.players.length,
            maxSeats: r.maxSeats,
            seatPrice: r.seatPrice,
            gameRound: r.gameRound || 0,
            playersList: r.players.map(p => ({
                userId: p.userId,
                username: p.username || p.telegramId,
                health: p.health,
                kills: p.kills || 0
            }))
        }));
        socket.emit('admin_rooms', { rooms: roomsList });
        
        // المباريات النشطة
        const games = Array.from(activeGames.entries()).map(([id, game]) => ({
            roomId: id,
            players: Array.from(game.tanks.keys()),
            aliveCount: game.aliveCount,
            bullets: game.bullets.length,
            duration: Math.floor((Date.now() - game.gameStartTime) / 1000),
            gameRound: game.gameRound || 0
        }));
        socket.emit('admin_active_games', { games });
        
    } catch (error) {
        console.error('Error sending admin data:', error);
    }
}

async function getAdminStats() {
    try {
        const result = await pool.query(`
            SELECT 
                COUNT(*) as total_users,
                SUM(balance) as total_balance,
                SUM(games_played) as total_games,
                SUM(wins) as total_wins,
                SUM(kills) as total_kills,
                AVG(elo) as average_elo,
                COUNT(CASE WHEN is_banned THEN 1 END) as banned_users,
                COUNT(CASE WHEN is_admin THEN 1 END) as admin_users
            FROM users
        `);
        
        const stats = {
            totalUsers: parseInt(result.rows[0]?.total_users) || 0,
            totalBalance: parseFloat(result.rows[0]?.total_balance) || 0,
            totalGames: parseInt(result.rows[0]?.total_games) || 0,
            totalWins: parseInt(result.rows[0]?.total_wins) || 0,
            totalKills: parseInt(result.rows[0]?.total_kills) || 0,
            averageELO: Math.round(result.rows[0]?.average_elo) || 1000,
            bannedUsers: parseInt(result.rows[0]?.banned_users) || 0,
            adminUsers: parseInt(result.rows[0]?.admin_users) || 0,
            onlinePlayers: players.size,
            activeRooms: rooms.size,
            activeGames: activeGames.size,
            monitoring: monitoring.getStats(),
            locks: lockSystem.getStats(),
            antiCheat: antiCheat.getStats(),
            database: db.getHealth()
        };
        
        const topResult = await pool.query(`
            SELECT id, username, elo, kills, wins, games_played
            FROM users
            ORDER BY elo DESC
            LIMIT 10
        `);
        
        stats.topPlayers = topResult.rows.map(row => ({
            id: row.id,
            username: row.username || 'لاعب',
            elo: row.elo || 1000,
            rank: ELO.getRank(row.elo || 1000),
            kills: row.kills || 0,
            wins: row.wins || 0,
            gamesPlayed: row.games_played || 0
        }));
        
        return stats;
    } catch (error) {
        console.error('Error getting admin stats:', error);
        return {};
    }
}

async function kickPlayer(socket, params) {
    const { userId, reason } = params;
    
    const player = Array.from(players.values()).find(p => p.userId === userId);
    if (!player) {
        socket?.emit('admin_error', { message: 'Player not found' });
        return;
    }
    
    const playerSocket = io.sockets.sockets.get(player.socketId);
    if (playerSocket) {
        playerSocket.emit('kicked', { reason: reason || 'تم طردك من الخادم' });
        playerSocket.disconnect();
    }
    
    socket?.emit('admin_message', { 
        message: `✅ تم طرد اللاعب ${userId}`,
        type: 'success'
    });
}

async function banPlayer(socket, params) {
    const { userId, duration, reason } = params;
    
    await pool.query(
        `UPDATE users SET 
         is_banned = TRUE,
         banned_until = $1,
         ban_reason = $2
         WHERE id = $3`,
        [new Date(Date.now() + (duration || 24 * 60 * 60 * 1000)), 
         reason || 'تم حظرك من قبل المدير', 
         userId]
    );
    
    // تحديث الكاش
    userCache.delete(userId);
    
    const player = Array.from(players.values()).find(p => p.userId === userId);
    if (player) {
        const playerSocket = io.sockets.sockets.get(player.socketId);
        if (playerSocket) {
            playerSocket.emit('banned', { 
                reason: reason || 'تم حظرك من قبل المدير',
                duration: duration || '24 ساعة'
            });
            playerSocket.disconnect();
        }
    }
    
    socket?.emit('admin_message', { 
        message: `✅ تم حظر اللاعب ${userId}`,
        type: 'success'
    });
}

async function setPlayerBalance(socket, params) {
    const { userId, amount, action } = params;
    
    const result = await pool.query('SELECT balance FROM users WHERE id = $1', [userId]);
    if (result.rows.length === 0) {
        socket?.emit('admin_error', { message: 'User not found' });
        return;
    }
    
    let newBalance = result.rows[0].balance || 0;
    
    if (action === 'set') {
        newBalance = amount;
    } else if (action === 'add') {
        newBalance += amount;
    } else if (action === 'subtract') {
        newBalance -= amount;
    }
    
    newBalance = Math.max(0, newBalance);
    await pool.query('UPDATE users SET balance = $1 WHERE id = $2', [newBalance, userId]);
    
    // تحديث الكاش
    userCache.delete(userId);
    
    const player = Array.from(players.values()).find(p => p.userId === userId);
    if (player) {
        const playerSocket = io.sockets.sockets.get(player.socketId);
        if (playerSocket) {
            playerSocket.emit('balance_update', {
                balance: newBalance,
                reason: 'تم تعديل رصيدك بواسطة المدير'
            });
        }
    }
    
    socket?.emit('admin_message', { 
        message: `✅ تم تعديل رصيد ${userId}: ${newBalance}$`,
        type: 'success'
    });
}

// ============================================
// 📡 API Routes
// ============================================

// نقطة نهاية للتحقق من صحة الخادم
app.get('/health', (req, res) => {
    const health = {
        status: 'ok',
        timestamp: new Date().toISOString(),
        uptime: monitoring.formatUptime(Math.floor((Date.now() - monitoring.startTime) / 1000)),
        database: db.getHealth(),
        connections: monitoring.metrics.connections.active,
        version: '9.0.0'
    };
    res.json(health);
});

// نقطة نهاية للحصول على إعدادات المظهر (للجميع)
app.get('/api/appearance', async (req, res) => {
    try {
        const config = await getServerConfig();
        res.json({
            success: true,
            appearance: config.appearance || {
                gameLogo: '/images/default/logo.png',
                backgroundImage: '/images/default/background.jpg'
            }
        });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

// نقطة نهاية للحصول على بيانات المستخدم (للجميع)
app.get('/api/user/:userId', async (req, res) => {
    try {
        const { userId } = req.params;
        const userData = await getUserData(userId);
        res.json({
            success: true,
            user: {
                id: userData.id,
                username: userData.username,
                balance: userData.balance || 0,
                elo: userData.elo || 1000,
                rank: ELO.getRank(userData.elo || 1000),
                kills: userData.kills || 0,
                wins: userData.wins || 0,
                gamesPlayed: userData.games_played || 0,
                isAdmin: userData.is_admin || false
            }
        });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

// ============================================
// 🚀 تشغيل الخادم
// ============================================
const PORT = process.env.PORT || 3000;
const db = new DatabaseManager();
let pool;

// بدء الخادم مع إعادة محاولة
async function startServer() {
    try {
        // الاتصال بقاعدة البيانات
        pool = await db.connect();
        
        // تهيئة قاعدة البيانات
        await initializeDatabase();
        
        // بدء الخادم
        server.listen(PORT, () => {
            console.log(`
╔══════════════════════════════════════════════════════════════════════════════╗
║                                                                              ║
║     🎮 BATTLE TANKS ROYALE - الإصدار النهائي v9.0.0 🎮                    ║
║                                                                              ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  📡 Server: http://localhost:${PORT}
║  🗄️ Database: PostgreSQL (Neon) ✅ Connected
║  ⚡ WebSocket: Ready
║  🎯 Mode: Battle Royale (Free-for-All)
║                                                                              ║
║  🛡️ Anti-Cheat: ${antiCheat.enabled ? '✅ ENABLED' : '❌ DISABLED'}
║  🔒 Lock System: ${lockSystem ? '✅ ACTIVE' : '❌ INACTIVE'}
║  📊 Queue System: ${actionProcessor ? '✅ ACTIVE' : '❌ INACTIVE'}
║  📈 Monitoring: ${monitoring ? '✅ ACTIVE' : '❌ INACTIVE'}
║                                                                              ║
║  🏠 Rooms: ${rooms.size} available
║  👥 Players: ${players.size} online
║  🎮 Games: ${activeGames.size} active
║  👑 Admin ID: 7011476249
║                                                                              ║
║  📊 API Endpoints:
║     - GET  /health
║     - GET  /api/appearance
║     - GET  /api/user/:userId
║                                                                              ║
║  🔄 Database Auto-Reconnect: ✅ ENABLED
║  ⏱️  Reconnect Delay: Exponential (5s - 60s)
║  🔁 Max Reconnect Attempts: Unlimited
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝
            `);
        });
        
        // مراقبة صحة قاعدة البيانات بشكل دوري
        setInterval(async () => {
            try {
                await pool.query('SELECT 1');
                if (!db.isConnected) {
                    console.log('✅ Database health check: connected');
                    db.isConnected = true;
                    monitoring.recordDatabaseStatus(true);
                }
            } catch (error) {
                if (db.isConnected) {
                    console.error('❌ Database health check failed:', error.message);
                    db.isConnected = false;
                    monitoring.recordDatabaseStatus(false, error.message);
                    db.connect().catch(() => {});
                }
            }
        }, 30000);
        
    } catch (error) {
        console.error('❌ Failed to start server:', error);
        console.log(`🔄 Retrying server start in 10 seconds...`);
        setTimeout(startServer, 10000);
    }
}

startServer();

// ============================================
// 🛑 معالجة الإغلاق الآمن
// ============================================
process.on('SIGTERM', () => {
    console.log('🛑 SIGTERM received, shutting down gracefully...');
    server.close(() => {
        console.log('✅ Server closed');
        if (pool) {
            pool.end(() => {
                console.log('✅ Database connection closed');
                process.exit(0);
            });
        } else {
            process.exit(0);
        }
    });
});

process.on('SIGINT', () => {
    console.log('🛑 SIGINT received, shutting down gracefully...');
    server.close(() => {
        console.log('✅ Server closed');
        if (pool) {
            pool.end(() => {
                console.log('✅ Database connection closed');
                process.exit(0);
            });
        } else {
            process.exit(0);
        }
    });
});

process.on('uncaughtException', (error) => {
    console.error('💥 Uncaught Exception:', error);
    monitoring.recordError('uncaught_exception', error.message);
});

process.on('unhandledRejection', (reason, promise) => {
    console.error('💥 Unhandled Rejection:', reason);
    monitoring.recordError('unhandled_rejection', reason);
});

module.exports = { server, io, app, pool, monitoring, lockSystem, antiCheat, actionProcessor, db };
