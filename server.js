// ============================================
// 🚀 BATTLE TANKS ROYALE - الخادم النهائي الكامل
// ============================================
// Version: 12.0.0 - COMPLETE EDITION
// جميع الميزات المطلوبة - جاهز للإنتاج الفوري
// ============================================

const express = require('express');
const http = require('http');
const socketIo = require('socket.io');
const { Pool } = require('pg');
const cors = require('cors');
const crypto = require('crypto');
const EventEmitter = require('events');
const helmet = require('helmet');
const morgan = require('morgan');
const winston = require('winston');
const jwt = require('jsonwebtoken');
const rateLimit = require('express-rate-limit');
const compression = require('compression');
const { v4: uuidv4 } = require('uuid');
const fs = require('fs');
const path = require('path');

// ============================================
// 📁 إنشاء مجلدات السجلات
// ============================================
const logsDir = path.join(__dirname, 'logs');
if (!fs.existsSync(logsDir)) {
    fs.mkdirSync(logsDir, { recursive: true });
}

// ============================================
// 🛡️ معالجة الأخطاء غير المتوقعة
// ============================================
process.on('uncaughtException', (err) => {
    console.error('❌ Uncaught Exception:', err.message);
    console.error(err.stack);
});

process.on('unhandledRejection', (reason, promise) => {
    console.error('❌ Unhandled Rejection:', reason);
});

// ============================================
// 📝 نظام التسجيل المتقدم
// ============================================
class AdvancedLogger {
    constructor() {
        this.currentLevel = process.env.LOG_LEVEL || 'info';
        this.loggers = {};
        
        const format = winston.format.combine(
            winston.format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss' }),
            winston.format.errors({ stack: true }),
            winston.format.metadata(),
            winston.format.json()
        );
        
        this.mainLogger = winston.createLogger({
            level: this.currentLevel,
            format,
            defaultMeta: {
                service: 'battle-tanks-royale',
                version: '12.0.0',
                pid: process.pid
            },
            transports: [
                new winston.transports.Console({
                    format: winston.format.combine(
                        winston.format.colorize(),
                        winston.format.printf(({ timestamp, level, message, metadata }) => {
                            const metaStr = metadata && Object.keys(metadata).length > 0 
                                ? JSON.stringify(metadata) 
                                : '';
                            return `${timestamp} [${level}] ${message} ${metaStr}`;
                        })
                    )
                }),
                new winston.transports.File({
                    filename: path.join(logsDir, 'error.log'),
                    level: 'error',
                    maxsize: 10485760,
                    maxFiles: 10,
                    tailable: true
                }),
                new winston.transports.File({
                    filename: path.join(logsDir, 'combined.log'),
                    maxsize: 10485760,
                    maxFiles: 10,
                    tailable: true
                }),
                new winston.transports.File({
                    filename: path.join(logsDir, 'audit.log'),
                    level: 'info',
                    maxsize: 10485760,
                    maxFiles: 5,
                    tailable: true
                })
            ]
        });
        
        console.log(`✅ Logger initialized (PID: ${process.pid})`);
    }
    
    info(message, metadata = {}) {
        this.mainLogger.info(message, { metadata });
    }
    
    error(message, metadata = {}) {
        this.mainLogger.error(message, { metadata });
    }
    
    warn(message, metadata = {}) {
        this.mainLogger.warn(message, { metadata });
    }
    
    debug(message, metadata = {}) {
        this.mainLogger.debug(message, { metadata });
    }
    
    audit(action, userId, details = {}) {
        this.mainLogger.info(`AUDIT: ${action}`, {
            metadata: { userId, action, details, timestamp: new Date().toISOString() }
        });
    }
}

const logger = new AdvancedLogger();

// ============================================
// 🔄 Circuit Breaker
// ============================================
class CircuitBreaker {
    constructor(options = {}) {
        this.failureThreshold = options.failureThreshold || 5;
        this.timeout = options.timeout || 60000;
        this.halfOpenTimeout = options.halfOpenTimeout || 30000;
        this.failureCount = 0;
        this.state = 'CLOSED';
        this.lastFailureTime = null;
        this.metrics = {
            totalRequests: 0,
            successfulRequests: 0,
            failedRequests: 0,
            rejectedRequests: 0
        };
    }
    
    async execute(action, fallback = null) {
        this.metrics.totalRequests++;
        
        if (this.state === 'OPEN') {
            this.metrics.rejectedRequests++;
            if (Date.now() - this.lastFailureTime > this.timeout) {
                this.state = 'HALF_OPEN';
                logger.info('🔄 Circuit breaker: HALF_OPEN');
            } else {
                if (fallback) return fallback();
                throw new Error('Circuit breaker is OPEN');
            }
        }
        
        try {
            const result = await action();
            if (this.state === 'HALF_OPEN') {
                this.reset();
                logger.info('✅ Circuit breaker: CLOSED');
            }
            this.metrics.successfulRequests++;
            return result;
        } catch (error) {
            this.metrics.failedRequests++;
            this.failureCount++;
            this.lastFailureTime = Date.now();
            
            if (this.state === 'HALF_OPEN' || this.failureCount >= this.failureThreshold) {
                this.state = 'OPEN';
                logger.warn(`⚠️ Circuit breaker: OPEN (${this.failureCount} failures)`);
            }
            
            if (fallback) return fallback();
            throw error;
        }
    }
    
    reset() {
        this.failureCount = 0;
        this.state = 'CLOSED';
        this.lastFailureTime = null;
    }
    
    getStats() {
        return {
            state: this.state,
            failureCount: this.failureCount,
            ...this.metrics,
            successRate: this.metrics.totalRequests > 0 
                ? (this.metrics.successfulRequests / this.metrics.totalRequests * 100).toFixed(2) + '%'
                : 'N/A'
        };
    }
}

// ============================================
// 📊 نظام المراقبة المتقدم
// ============================================
class AdvancedMonitoring {
    constructor() {
        this.metrics = {
            connections: { total: 0, active: 0, peak: 0, historical: [] },
            requests: {
                total: 0,
                success: 0,
                error: 0,
                rate: 0,
                byEndpoint: {},
                byStatus: {},
                byMethod: {},
                responseTime: { avg: 0, max: 0, min: Infinity }
            },
            games: {
                total: 0,
                active: 0,
                completed: 0,
                averageDuration: 0,
                maxPlayers: 0
            },
            errors: {
                total: 0,
                byType: {},
                byService: {},
                recent: [],
                rate: 0
            },
            database: {
                connected: false,
                reconnectAttempts: 0,
                lastError: null,
                queryCount: 0,
                avgQueryTime: 0
            },
            admin: { logins: 0, actions: 0, failedLogins: 0 },
            system: {
                cpu: 0,
                memory: 0,
                uptime: 0
            },
            business: {
                totalUsers: 0,
                activeUsers: 0,
                totalBalance: 0,
                totalKills: 0,
                totalWins: 0
            }
        };
        
        this.startTime = Date.now();
        this.requestTimestamps = [];
        this.errorLogs = [];
        this.maxErrorLogs = 100;
        this.responseTimeBuffer = [];
        this.maxBufferSize = 1000;
        this.metricsInterval = null;
        
        this.startMetricsCollection();
    }
    
    startMetricsCollection() {
        this.metricsInterval = setInterval(() => {
            this.collectSystemMetrics();
            this.calculatePercentiles();
        }, 30000);
    }
    
    collectSystemMetrics() {
        const mem = process.memoryUsage();
        this.metrics.system.memory = mem.heapUsed / 1024 / 1024;
        this.metrics.system.cpu = process.cpuUsage().user / 1000000;
        this.metrics.system.uptime = Math.floor((Date.now() - this.startTime) / 1000);
    }
    
    calculatePercentiles() {
        if (this.responseTimeBuffer.length === 0) return;
        const sorted = [...this.responseTimeBuffer].sort((a, b) => a - b);
        const len = sorted.length;
        if (len > 0) {
            this.metrics.requests.responseTime.p95 = sorted[Math.floor(len * 0.95)] || 0;
            this.metrics.requests.responseTime.p99 = sorted[Math.floor(len * 0.99)] || 0;
        }
    }
    
    recordConnection(type) {
        if (type === 'connect') {
            this.metrics.connections.active++;
            this.metrics.connections.total++;
            if (this.metrics.connections.active > this.metrics.connections.peak) {
                this.metrics.connections.peak = this.metrics.connections.active;
            }
            this.metrics.connections.historical.push({
                timestamp: Date.now(),
                active: this.metrics.connections.active
            });
        } else {
            this.metrics.connections.active = Math.max(0, this.metrics.connections.active - 1);
        }
    }
    
    recordRequest(success, duration, endpoint = 'unknown', method = 'GET', status = 200) {
        this.metrics.requests.total++;
        if (success) this.metrics.requests.success++;
        else this.metrics.requests.error++;
        
        this.metrics.requests.byEndpoint[endpoint] = (this.metrics.requests.byEndpoint[endpoint] || 0) + 1;
        this.metrics.requests.byStatus[status] = (this.metrics.requests.byStatus[status] || 0) + 1;
        this.metrics.requests.byMethod[method] = (this.metrics.requests.byMethod[method] || 0) + 1;
        
        this.requestTimestamps.push(Date.now());
        if (this.requestTimestamps.length > 1000) this.requestTimestamps.shift();
        
        const oneMinuteAgo = Date.now() - 60000;
        const recentRequests = this.requestTimestamps.filter(t => t > oneMinuteAgo);
        this.metrics.requests.rate = recentRequests.length / 60;
        
        if (duration) {
            this.responseTimeBuffer.push(duration);
            const avg = (this.metrics.requests.responseTime.avg * 0.9) + (duration * 0.1);
            this.metrics.requests.responseTime.avg = Math.round(avg);
            if (duration > this.metrics.requests.responseTime.max) {
                this.metrics.requests.responseTime.max = Math.round(duration);
            }
            if (duration < this.metrics.requests.responseTime.min) {
                this.metrics.requests.responseTime.min = Math.round(duration);
            }
        }
    }
    
    recordError(errorType, errorDetails = null, service = 'unknown', userId = null) {
        this.metrics.errors.total++;
        this.metrics.errors.byType[errorType] = (this.metrics.errors.byType[errorType] || 0) + 1;
        this.metrics.errors.byService[service] = (this.metrics.errors.byService[service] || 0) + 1;
        
        const errorLog = { type: errorType, timestamp: Date.now(), details: errorDetails, service, userId };
        this.errorLogs.push(errorLog);
        if (this.errorLogs.length > this.maxErrorLogs) this.errorLogs.shift();
        
        logger.error(`Error recorded: ${errorType}`, { errorType, errorDetails, service, userId });
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
    
    recordDatabaseQuery(duration) {
        this.metrics.database.queryCount++;
        this.metrics.database.avgQueryTime = (this.metrics.database.avgQueryTime * 0.9) + (duration * 0.1);
    }
    
    recordAdminAction(type, userId = null) {
        if (type === 'login') this.metrics.admin.logins++;
        else if (type === 'login_failed') this.metrics.admin.failedLogins++;
        else this.metrics.admin.actions++;
        if (userId) logger.audit(type, userId);
    }
    
    recordGameStarted(players) {
        this.metrics.games.total++;
        this.metrics.games.active++;
        if (players > this.metrics.games.maxPlayers) this.metrics.games.maxPlayers = players;
    }
    
    recordGameEnded(duration) {
        this.metrics.games.active = Math.max(0, this.metrics.games.active - 1);
        this.metrics.games.completed++;
        this.metrics.games.averageDuration = (this.metrics.games.averageDuration * 0.9) + (duration * 0.1);
    }
    
    getStats() {
        const uptime = Math.floor((Date.now() - this.startTime) / 1000);
        return {
            ...this.metrics,
            uptime,
            uptimeFormatted: this.formatUptime(uptime),
            errorLogs: this.errorLogs.slice(-10),
            timestamp: new Date().toISOString()
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
            errors: { total: this.metrics.errors.total, recent: this.errorLogs.slice(-5) },
            system: this.metrics.system,
            timestamp: new Date().toISOString()
        };
    }
    
    stop() {
        if (this.metricsInterval) {
            clearInterval(this.metricsInterval);
            this.metricsInterval = null;
        }
    }
}

const monitoring = new AdvancedMonitoring();

// ============================================
// 🗄️ إدارة قاعدة البيانات المتقدمة
// ============================================
class AdvancedDatabaseManager {
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
        this.circuitBreaker = new CircuitBreaker({
            failureThreshold: 3,
            timeout: 30000,
            halfOpenTimeout: 10000
        });
        this.poolConfig = {
            max: parseInt(process.env.DB_POOL_MAX) || 20,
            idleTimeoutMillis: 30000,
            connectionTimeoutMillis: 15000,
            maxUses: 7500
        };
        this.preparedStatements = new Map();
        
        if (!this.connectionString) {
            logger.error('DATABASE_URL is not set, using default');
            this.connectionString = 'postgresql://neondb_owner:npg_MSOwr97htVJu@ep-patient-dawn-awed2uh0-pooler.c-12.us-east-1.aws.neon.tech/neondb?sslmode=require';
        }
        
        logger.info('Advanced Database Manager initialized');
    }
    
    async connect() {
        if (this.isReconnecting) {
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
            logger.info(`Connecting to database (attempt ${this.reconnectAttempts + 1})...`);
            
            this.pool = new Pool({
                connectionString: this.connectionString,
                ssl: { rejectUnauthorized: false },
                ...this.poolConfig
            });
            
            this.pool.on('error', (err) => {
                logger.error('Database pool error:', { error: err.message });
                this.isConnected = false;
                monitoring.recordDatabaseStatus(false, err.message);
                this.handleReconnect();
            });
            
            const startTime = Date.now();
            await this.circuitBreaker.execute(async () => {
                const client = await this.pool.connect();
                await client.query('SELECT 1');
                client.release();
            });
            const duration = Date.now() - startTime;
            
            this.isConnected = true;
            this.reconnectAttempts = 0;
            this.isReconnecting = false;
            this.reconnectDelay = 5000;
            this.lastError = null;
            monitoring.recordDatabaseStatus(true);
            monitoring.recordDatabaseQuery(duration);
            
            await this.initializeSchema();
            
            if (this.pingInterval) clearInterval(this.pingInterval);
            this.pingInterval = setInterval(() => this.healthCheck(), 15000);
            
            logger.info('✅ Database connected successfully');
            return this.pool;
            
        } catch (error) {
            logger.error('Database connection failed:', { error: error.message });
            this.isConnected = false;
            this.isReconnecting = false;
            this.lastError = error.message;
            monitoring.recordDatabaseStatus(false, error.message);
            monitoring.recordError('database_connection_error', error.message, 'database');
            return this.handleReconnect();
        }
    }
    
    async initializeSchema() {
        try {
            const schema = `
                CREATE TABLE IF NOT EXISTS users (
                    id VARCHAR(64) PRIMARY KEY,
                    telegram_id VARCHAR(64) UNIQUE,
                    username VARCHAR(100),
                    balance INTEGER DEFAULT 100,
                    elo INTEGER DEFAULT 1000,
                    kills INTEGER DEFAULT 0,
                    wins INTEGER DEFAULT 0,
                    games_played INTEGER DEFAULT 0,
                    total_rewards INTEGER DEFAULT 0,
                    is_admin BOOLEAN DEFAULT FALSE,
                    is_banned BOOLEAN DEFAULT FALSE,
                    ban_reason TEXT,
                    banned_until TIMESTAMP,
                    banned_by VARCHAR(64),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_login TIMESTAMP,
                    last_game TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                
                CREATE TABLE IF NOT EXISTS rooms (
                    id VARCHAR(64) PRIMARY KEY,
                    type VARCHAR(32),
                    name VARCHAR(100),
                    max_seats INTEGER DEFAULT 8,
                    seat_price INTEGER DEFAULT 1,
                    reward_multiplier FLOAT DEFAULT 1.0,
                    status VARCHAR(32) DEFAULT 'waiting',
                    players JSONB DEFAULT '[]',
                    spectators JSONB DEFAULT '[]',
                    game_round INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    stats JSONB DEFAULT '{}'
                );
                
                CREATE TABLE IF NOT EXISTS matches (
                    id VARCHAR(64) PRIMARY KEY,
                    room_id VARCHAR(64),
                    winner_id VARCHAR(64),
                    players JSONB,
                    kill_feed JSONB,
                    start_time TIMESTAMP,
                    end_time TIMESTAMP,
                    duration INTEGER,
                    total_players INTEGER,
                    game_round INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                
                CREATE TABLE IF NOT EXISTS server_config (
                    key VARCHAR(64) PRIMARY KEY,
                    value JSONB,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                
                CREATE TABLE IF NOT EXISTS visual_settings (
                    id VARCHAR(64) PRIMARY KEY,
                    event_key VARCHAR(64) UNIQUE,
                    image_url TEXT,
                    alt_text VARCHAR(255),
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                
                CREATE TABLE IF NOT EXISTS transactions (
                    id VARCHAR(64) PRIMARY KEY,
                    user_id VARCHAR(64),
                    type VARCHAR(32),
                    amount INTEGER,
                    balance_before INTEGER,
                    balance_after INTEGER,
                    description TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                
                CREATE INDEX IF NOT EXISTS idx_users_elo ON users(elo DESC);
                CREATE INDEX IF NOT EXISTS idx_users_wins ON users(wins DESC);
                CREATE INDEX IF NOT EXISTS idx_users_kills ON users(kills DESC);
                CREATE INDEX IF NOT EXISTS idx_matches_room ON matches(room_id);
                CREATE INDEX IF NOT EXISTS idx_matches_winner ON matches(winner_id);
                CREATE INDEX IF NOT EXISTS idx_transactions_user ON transactions(user_id);
                CREATE INDEX IF NOT EXISTS idx_transactions_created ON transactions(created_at DESC);
            `;
            
            await this.query(schema);
            logger.info('✅ Database schema initialized');
        } catch (error) {
            logger.error('Error initializing schema:', { error: error.message });
            throw error;
        }
    }
    
    async healthCheck() {
        try {
            const startTime = Date.now();
            await this.circuitBreaker.execute(async () => {
                if (this.pool) {
                    const client = await this.pool.connect();
                    await client.query('SELECT 1');
                    client.release();
                }
            });
            const duration = Date.now() - startTime;
            
            if (!this.isConnected) {
                logger.info('Database health check: recovered');
                this.isConnected = true;
                monitoring.recordDatabaseStatus(true);
            }
            monitoring.recordDatabaseQuery(duration);
        } catch (error) {
            if (this.isConnected) {
                logger.error('Database health check failed:', { error: error.message });
                this.isConnected = false;
                monitoring.recordDatabaseStatus(false, error.message);
                monitoring.recordError('health_check_failed', error.message, 'database');
                this.handleReconnect();
            }
        }
    }
    
    async handleReconnect() {
        if (this.isReconnecting) return this.pool;
        
        this.isReconnecting = true;
        this.reconnectAttempts++;
        
        const delay = Math.min(
            this.reconnectDelay * Math.pow(1.5, this.reconnectAttempts - 1),
            this.maxReconnectDelay
        );
        
        logger.warn(`Reconnecting in ${Math.round(delay/1000)}s...`);
        
        return new Promise((resolve) => {
            if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
            this.reconnectTimer = setTimeout(async () => {
                try {
                    await this.connect();
                    resolve(this.pool);
                } catch (error) {
                    logger.error(`Reconnection failed:`, { error: error.message });
                    this.isReconnecting = false;
                    this.handleReconnect().then(resolve);
                }
            }, delay);
        });
    }
    
    async query(text, params) {
        if (!this.isConnected || !this.pool) {
            await this.connect();
        }
        
        const startTime = Date.now();
        try {
            const result = await this.circuitBreaker.execute(async () => {
                return await this.pool.query(text, params);
            });
            
            const duration = Date.now() - startTime;
            monitoring.recordDatabaseQuery(duration);
            
            if (duration > 1000) {
                logger.warn('Slow query detected:', { query: text.substring(0, 100), duration });
            }
            
            return result;
        } catch (error) {
            logger.error('Database query error:', { error: error.message, query: text.substring(0, 100) });
            
            if (error.code === 'ECONNRESET' || error.code === '57P01' || error.code === '08003' ||
                error.code === '08006' || error.message.includes('connection')) {
                this.isConnected = false;
                monitoring.recordDatabaseStatus(false, error.message);
                await this.connect();
                return await this.pool.query(text, params);
            }
            
            monitoring.recordError('query_error', error.message, 'database');
            throw error;
        }
    }
    
    async transaction(callback) {
        if (!this.isConnected || !this.pool) await this.connect();
        
        const client = await this.pool.connect();
        try {
            await client.query('BEGIN');
            const result = await callback(client);
            await client.query('COMMIT');
            return result;
        } catch (error) {
            await client.query('ROLLBACK');
            logger.error('Transaction error:', { error: error.message });
            monitoring.recordError('transaction_error', error.message, 'database');
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
            lastError: this.lastError,
            circuitBreaker: {
                state: this.circuitBreaker.state,
                failureCount: this.circuitBreaker.failureCount
            }
        };
    }
    
    async shutdown() {
        if (this.pingInterval) clearInterval(this.pingInterval);
        if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
        if (this.pool) await this.pool.end();
        logger.info('Database pool closed');
    }
}

const db = new AdvancedDatabaseManager();

// ============================================
// 🔒 نظام القفل الموزع
// ============================================
class DistributedLock {
    constructor() {
        this.locks = new Map();
        this.waitingQueues = new Map();
        this.lockTimeouts = new Map();
        this.maxLockTime = 30000;
        this.cleanupInterval = setInterval(() => this.cleanup(), 60000);
        this.metrics = {
            totalLocks: 0,
            activeLocks: 0,
            totalWaits: 0,
            avgWaitTime: 0,
            lockTimeouts: 0
        };
        logger.info('Distributed Lock initialized');
    }
    
    async acquireLock(resourceId, userId, timeout = 10000) {
        const lockKey = `lock:${resourceId}`;
        const startTime = Date.now();
        this.metrics.totalLocks++;
        
        return new Promise((resolve, reject) => {
            if (this.locks.has(lockKey)) {
                if (!this.waitingQueues.has(lockKey)) {
                    this.waitingQueues.set(lockKey, []);
                }
                
                const queue = this.waitingQueues.get(lockKey);
                const timeoutId = setTimeout(() => {
                    const index = queue.findIndex(item => item.userId === userId);
                    if (index !== -1) {
                        queue.splice(index, 1);
                        this.metrics.totalWaits++;
                        reject(new Error('Lock acquisition timeout'));
                    }
                }, timeout);
                
                const startWait = Date.now();
                queue.push({ userId, resolve, reject, timeoutId, startWait });
                return;
            }
            
            this.grantLock(lockKey, userId, resolve);
        });
    }
    
    grantLock(lockKey, userId, resolve) {
        this.locks.set(lockKey, {
            userId,
            acquiredAt: Date.now(),
            expiresAt: Date.now() + this.maxLockTime
        });
        this.metrics.activeLocks++;
        
        const timeoutId = setTimeout(() => {
            logger.warn(`Lock for ${lockKey} expired automatically`);
            this.releaseLock(lockKey);
            this.metrics.lockTimeouts++;
        }, this.maxLockTime);
        
        this.lockTimeouts.set(lockKey, timeoutId);
        resolve(true);
    }
    
    releaseLock(lockKey) {
        if (!this.locks.has(lockKey)) return false;
        
        this.locks.delete(lockKey);
        this.metrics.activeLocks = Math.max(0, this.metrics.activeLocks - 1);
        
        if (this.lockTimeouts.has(lockKey)) {
            clearTimeout(this.lockTimeouts.get(lockKey));
            this.lockTimeouts.delete(lockKey);
        }
        
        if (this.waitingQueues.has(lockKey)) {
            const queue = this.waitingQueues.get(lockKey);
            if (queue.length > 0) {
                const next = queue.shift();
                clearTimeout(next.timeoutId);
                const waitTime = Date.now() - next.startWait;
                this.metrics.avgWaitTime = (this.metrics.avgWaitTime * 0.9) + (waitTime * 0.1);
                this.grantLock(lockKey, next.userId, next.resolve);
            } else {
                this.waitingQueues.delete(lockKey);
            }
        }
        
        return true;
    }
    
    cleanup() {
        const now = Date.now();
        for (const [key, lock] of this.locks) {
            if (lock.expiresAt < now) {
                logger.warn(`Cleaning up expired lock: ${key}`);
                this.releaseLock(key);
                this.metrics.lockTimeouts++;
            }
        }
    }
    
    getStats() {
        return {
            activeLocks: this.metrics.activeLocks,
            waitingQueues: this.waitingQueues.size,
            totalWaiting: Array.from(this.waitingQueues.values()).reduce((sum, q) => sum + q.length, 0),
            ...this.metrics
        };
    }
    
    shutdown() {
        if (this.cleanupInterval) clearInterval(this.cleanupInterval);
    }
}

const lockSystem = new DistributedLock();

// ============================================
// 🛡️ نظام الحماية المتقدم
// ============================================
class AntiCheatSystem {
    constructor() {
        this.actionTracker = new Map();
        this.rateLimits = {
            move: { max: 60, window: 1000, blockTime: 5000, penalty: 1 },
            shoot: { max: 5, window: 3000, blockTime: 10000, penalty: 2 },
            join: { max: 5, window: 10000, blockTime: 30000, penalty: 1 },
            auth: { max: 5, window: 5000, blockTime: 60000, penalty: 3 },
            admin: { max: 10, window: 60000, blockTime: 300000, penalty: 2 }
        };
        
        this.suspiciousActivity = new Map();
        this.bannedUsers = new Set();
        this.bannedIPs = new Set();
        this.enabled = process.env.ANTI_CHEAT_ENABLED !== 'false';
        this.adminPassword = process.env.ADMIN_PASSWORD || 'Admin@2024#Battle';
        this.banDuration = 24 * 60 * 60 * 1000;
        this.bannedCache = new Map();
        this.cacheTTL = 60000;
        this.blockedUntil = new Map();
        this.threatScores = new Map();
        
        logger.info('AntiCheat System initialized', { enabled: this.enabled });
    }
    
    checkRateLimit(userId, actionType, ip = null) {
        if (!this.enabled) return true;
        if (this.bannedUsers.has(userId) || (ip && this.bannedIPs.has(ip))) return false;
        
        const now = Date.now();
        const limit = this.rateLimits[actionType];
        if (!limit) return true;
        
        const key = `${userId}:${actionType}`;
        if (this.blockedUntil.has(key) && this.blockedUntil.get(key) > now) {
            return false;
        }
        
        if (!this.actionTracker.has(key)) {
            this.actionTracker.set(key, { actions: [], lastReset: now, penalties: 0 });
        }
        
        const tracker = this.actionTracker.get(key);
        if (now - tracker.lastReset > limit.window) {
            tracker.actions = [];
            tracker.lastReset = now;
            tracker.penalties = Math.max(0, tracker.penalties - 1);
        }
        
        tracker.actions.push(now);
        tracker.actions = tracker.actions.filter(t => now - t < limit.window);
        
        const effectiveMax = limit.max - (tracker.penalties * limit.penalty);
        if (tracker.actions.length > effectiveMax) {
            const blockTime = limit.blockTime * (1 + tracker.penalties * 0.5);
            this.blockedUntil.set(key, now + blockTime);
            tracker.penalties++;
            this.reportSuspiciousActivity(userId, `Rate limit exceeded: ${actionType}`);
            return false;
        }
        
        return true;
    }
    
    reportSuspiciousActivity(userId, reason, ip = null) {
        if (!this.suspiciousActivity.has(userId)) {
            this.suspiciousActivity.set(userId, {
                reports: [],
                warnings: 0,
                lastReport: Date.now(),
                severity: 0,
                firstSeen: Date.now()
            });
        }
        
        const activity = this.suspiciousActivity.get(userId);
        const severity = this.calculateSeverity(reason);
        
        activity.reports.push({ reason, timestamp: Date.now(), severity, ip });
        activity.warnings++;
        activity.severity += severity;
        activity.lastReport = Date.now();
        
        this.updateThreatScore(userId, severity);
        
        const threatScore = this.threatScores.get(userId) || 0;
        if (threatScore >= 50) {
            this.banUser(userId, 'نشاط مشبوه خطير (تلقائي)', ip);
            return true;
        } else if (threatScore >= 30) {
            this.blockedUntil.set(`temp_${userId}`, Date.now() + 600000);
            return true;
        }
        
        return false;
    }
    
    calculateSeverity(reason) {
        if (reason.includes('aimbot') || reason.includes('godmode')) return 5;
        if (reason.includes('speedhack') || reason.includes('teleport')) return 4;
        if (reason.includes('spam') || reason.includes('flood')) return 2;
        return 1;
    }
    
    updateThreatScore(userId, severity) {
        const current = this.threatScores.get(userId) || 0;
        const newScore = current * 0.9 + severity * 2;
        this.threatScores.set(userId, Math.min(100, newScore));
    }
    
    async banUser(userId, reason, ip = null) {
        if (this.bannedUsers.has(userId)) return;
        
        this.bannedUsers.add(userId);
        if (ip) this.bannedIPs.add(ip);
        this.bannedCache.set(userId, { reason, timestamp: Date.now() });
        this.threatScores.delete(userId);
        
        logger.warn(`User banned: ${userId} - ${reason}`, { userId, reason, ip });
        
        try {
            await db.query(
                `UPDATE users SET is_banned = TRUE, ban_reason = $1, banned_until = $2, banned_by = 'system'
                 WHERE id = $3`,
                [reason, new Date(Date.now() + this.banDuration), userId]
            );
        } catch (error) {
            logger.error('Error banning user:', { error: error.message, userId });
        }
    }
    
    async isUserBanned(userId) {
        if (this.bannedUsers.has(userId)) return true;
        if (this.bannedCache.has(userId)) {
            const cached = this.bannedCache.get(userId);
            if (Date.now() - cached.timestamp < this.cacheTTL) return true;
            this.bannedCache.delete(userId);
        }
        
        try {
            const result = await db.query('SELECT is_banned, banned_until FROM users WHERE id = $1', [userId]);
            if (result.rows.length > 0) {
                const user = result.rows[0];
                if (user.is_banned && user.banned_until && new Date(user.banned_until) > new Date()) {
                    this.bannedUsers.add(userId);
                    this.bannedCache.set(userId, { reason: 'Banned until ' + user.banned_until, timestamp: Date.now() });
                    return true;
                }
            }
        } catch (error) {
            logger.error('Error checking ban status:', { error: error.message, userId });
        }
        return false;
    }
    
    verifyAdminPassword(password) {
        return password === this.adminPassword;
    }
    
    getStats() {
        return {
            bannedUsers: this.bannedUsers.size,
            bannedIPs: this.bannedIPs.size,
            suspiciousActivities: this.suspiciousActivity.size,
            activeTrackers: this.actionTracker.size,
            enabled: this.enabled,
            blocked: this.blockedUntil.size,
            threatScores: Array.from(this.threatScores.entries()).slice(0, 20)
        };
    }
    
    async unbanUser(userId) {
        this.bannedUsers.delete(userId);
        this.bannedCache.delete(userId);
        this.threatScores.delete(userId);
        
        try {
            await db.query(`UPDATE users SET is_banned = FALSE, ban_reason = NULL, banned_until = NULL WHERE id = $1`, [userId]);
            logger.info(`User unbanned: ${userId}`);
            return true;
        } catch (error) {
            logger.error('Error unbanning user:', { error: error.message, userId });
            return false;
        }
    }
}

const antiCheat = new AntiCheatSystem();

// ============================================
// 📦 التخزين المؤقت المتقدم
// ============================================
class CacheManager {
    constructor() {
        this.caches = {
            memory: new Map(),
            user: new Map(),
            leaderboard: new Map(),
            visual: new Map(),
            config: new Map(),
            game: new Map(),
            session: new Map()
        };
        
        this.ttls = {
            memory: 30000,
            user: 60000,
            leaderboard: 30000,
            visual: 300000,
            config: 600000,
            game: 10000,
            session: 1800000
        };
        
        this.cacheHits = 0;
        this.cacheMisses = 0;
        this.cacheSize = 0;
        this.evictions = 0;
        this.maxCacheSize = 10000;
        this.cleanupInterval = setInterval(() => this.cleanup(), 60000);
        
        logger.info('Cache Manager initialized');
    }
    
    async get(key, cacheType = 'memory') {
        this.cacheSize = this.getTotalSize();
        const cache = this.caches[cacheType];
        if (!cache) return null;
        
        const entry = cache.get(key);
        if (!entry) {
            this.cacheMisses++;
            return null;
        }
        
        if (Date.now() - entry.timestamp > (entry.ttl || this.ttls[cacheType] || 60000)) {
            cache.delete(key);
            this.evictions++;
            this.cacheMisses++;
            return null;
        }
        
        this.cacheHits++;
        return entry.value;
    }
    
    async set(key, value, cacheType = 'memory', ttl = null) {
        const ttlMs = ttl || this.ttls[cacheType] || 60000;
        const cache = this.caches[cacheType];
        if (!cache) return false;
        
        if (this.cacheSize >= this.maxCacheSize) this.evictOldest();
        
        cache.set(key, { value, timestamp: Date.now(), ttl: ttlMs });
        this.cacheSize = this.getTotalSize();
        return true;
    }
    
    async delete(key, cacheType = 'memory') {
        const cache = this.caches[cacheType];
        if (cache) {
            cache.delete(key);
            this.cacheSize = this.getTotalSize();
        }
    }
    
    evictOldest() {
        let oldest = null;
        let oldestTime = Infinity;
        
        for (const [type, cache] of Object.entries(this.caches)) {
            for (const [key, entry] of cache) {
                if (entry.timestamp < oldestTime) {
                    oldestTime = entry.timestamp;
                    oldest = { type, key };
                }
            }
        }
        
        if (oldest) {
            this.caches[oldest.type].delete(oldest.key);
            this.evictions++;
        }
    }
    
    getTotalSize() {
        let size = 0;
        for (const cache of Object.values(this.caches)) size += cache.size;
        return size;
    }
    
    cleanup() {
        const now = Date.now();
        for (const [type, cache] of Object.entries(this.caches)) {
            for (const [key, entry] of cache) {
                if (now - entry.timestamp > (entry.ttl || this.ttls[type] || 60000)) {
                    cache.delete(key);
                    this.evictions++;
                }
            }
        }
        this.cacheSize = this.getTotalSize();
    }
    
    getStats() {
        return {
            size: this.cacheSize,
            maxSize: this.maxCacheSize,
            hits: this.cacheHits,
            misses: this.cacheMisses,
            hitRate: (this.cacheHits + this.cacheMisses) > 0
                ? (this.cacheHits / (this.cacheHits + this.cacheMisses) * 100).toFixed(2) + '%'
                : 'N/A',
            evictions: this.evictions,
            caches: Object.fromEntries(Object.entries(this.caches).map(([type, cache]) => [type, cache.size]))
        };
    }
    
    shutdown() {
        if (this.cleanupInterval) clearInterval(this.cleanupInterval);
    }
}

const cache = new CacheManager();

// ============================================
// 🔌 معالج الطوابير
// ============================================
class QueueProcessor {
    constructor() {
        this.queues = { high: [], normal: [], low: [], dead: [] };
        this.processing = { high: false, normal: false, low: false };
        this.maxConcurrent = { high: 20, normal: 10, low: 5 };
        this.timeout = 30000;
        this.stats = {
            totalProcessed: 0,
            totalErrors: 0,
            averageProcessingTime: 0,
            peakQueueSize: 0,
            byPriority: { high: 0, normal: 0, low: 0 },
            retries: 0
        };
        this.processingTimes = [];
        this.retryDelays = [1000, 5000, 15000];
        this.idempotencyCache = new Map();
        logger.info('Queue Processor initialized');
    }
    
    async add(action, priority = 0, idempotencyKey = null) {
        return new Promise((resolve, reject) => {
            if (idempotencyKey && this.idempotencyCache.has(idempotencyKey)) {
                const cached = this.idempotencyCache.get(idempotencyKey);
                if (Date.now() - cached.timestamp < 60000) {
                    resolve(cached.result);
                    return;
                }
            }
            
            const priorityLevel = priority > 0 ? 'high' : priority === 0 ? 'normal' : 'low';
            const item = {
                id: idempotencyKey || `action_${Date.now()}_${uuidv4().slice(0, 8)}`,
                action,
                priority,
                resolve,
                reject,
                timestamp: Date.now(),
                retries: 0,
                maxRetries: 3,
                status: 'pending',
                priorityLevel,
                startTime: null,
                endTime: null
            };
            
            this.queues[priorityLevel].push(item);
            this.stats.byPriority[priorityLevel]++;
            
            if (this.getTotalSize() > this.stats.peakQueueSize) {
                this.stats.peakQueueSize = this.getTotalSize();
            }
            
            this.process(priorityLevel);
        });
    }
    
    async process(priorityLevel = null) {
        const levels = priorityLevel ? [priorityLevel] : ['high', 'normal', 'low'];
        
        for (const level of levels) {
            if (this.processing[level] || this.queues[level].length === 0) continue;
            
            this.processing[level] = true;
            while (this.queues[level].length > 0) {
                const batch = this.queues[level].splice(0, this.maxConcurrent[level]);
                await Promise.all(batch.map(item => this.processItem(item, level)));
            }
            this.processing[level] = false;
        }
    }
    
    async processItem(item, level) {
        item.startTime = Date.now();
        
        try {
            if (Date.now() - item.timestamp > this.timeout) {
                item.reject(new Error('Action timeout'));
                this.stats.totalErrors++;
                return;
            }
            
            const result = await item.action();
            this.stats.totalProcessed++;
            item.status = 'completed';
            item.endTime = Date.now();
            item.resolve(result);
            
            if (item.id) {
                this.idempotencyCache.set(item.id, { result, timestamp: Date.now() });
                for (const [key, value] of this.idempotencyCache) {
                    if (Date.now() - value.timestamp > 60000) this.idempotencyCache.delete(key);
                }
            }
            
            const duration = Date.now() - item.startTime;
            this.processingTimes.push(duration);
            if (this.processingTimes.length > 1000) this.processingTimes.shift();
            this.stats.averageProcessingTime = 
                this.processingTimes.reduce((a, b) => a + b, 0) / this.processingTimes.length;
            
        } catch (error) {
            this.stats.totalErrors++;
            item.status = 'failed';
            
            if (item.retries < item.maxRetries) {
                const delay = this.retryDelays[item.retries] || 5000;
                item.retries++;
                item.timestamp = Date.now() + delay;
                this.stats.retries++;
                
                setTimeout(() => {
                    this.queues[level].push(item);
                    this.process(level);
                }, delay);
            } else {
                this.queues.dead.push({ ...item, error: error.message, failedAt: Date.now() });
                item.reject(error);
            }
        }
    }
    
    getTotalSize() {
        return this.queues.high.length + this.queues.normal.length + this.queues.low.length + this.queues.dead.length;
    }
    
    getStats() {
        return {
            queueLength: this.getTotalSize(),
            processing: this.processing,
            maxConcurrent: this.maxConcurrent,
            ...this.stats,
            deadLetterCount: this.queues.dead.length,
            idempotencyCache: this.idempotencyCache.size
        };
    }
}

const queueProcessor = new QueueProcessor();

// ============================================
// 🎮 نظام ELO المتقدم
// ============================================
class ELOSystem {
    constructor() {
        this.K_FACTOR = 32;
        this.DEFAULT_ELO = 1000;
        this.MIN_ELO = 100;
        this.MAX_ELO = 3000;
        
        this.ranks = [
            { name: 'برونزي', minElo: 0, color: '#cd7f32', icon: '🥉', tier: 1 },
            { name: 'فضي', minElo: 1000, color: '#c0c0c0', icon: '🥈', tier: 2 },
            { name: 'ذهبي', minElo: 1200, color: '#ffd700', icon: '🥇', tier: 3 },
            { name: 'بلاتيني', minElo: 1400, color: '#e5e4e2', icon: '💎', tier: 4 },
            { name: 'ماسي', minElo: 1600, color: '#b9f2ff', icon: '👑', tier: 5 },
            { name: 'أسطوري', minElo: 1800, color: '#ff6b6b', icon: '⚡', tier: 6 },
            { name: 'خارق', minElo: 2000, color: '#ff00ff', icon: '🌟', tier: 7 }
        ];
        
        this.achievements = {
            first_win: { name: 'الانتصار الأول', points: 10, icon: '🏆' },
            win_streak_3: { name: 'سلسلة انتصارات (3)', points: 20, icon: '🔥' },
            win_streak_5: { name: 'سلسلة انتصارات (5)', points: 50, icon: '⚡' },
            win_streak_10: { name: 'سلسلة انتصارات (10)', points: 100, icon: '👑' },
            kill_streak_5: { name: 'سلسلة إقصاءات (5)', points: 15, icon: '💀' },
            kill_streak_10: { name: 'سلسلة إقصاءات (10)', points: 35, icon: '☠️' },
            perfect_game: { name: 'مباراة مثالية', points: 25, icon: '⭐' },
            veteran: { name: 'محارب مخضرم (100 مباراة)', points: 30, icon: '🎖️' },
            legend: { name: 'أسطورة (500 مباراة)', points: 50, icon: '👑' },
            top_10: { name: 'أفضل 10 لاعبين', points: 40, icon: '🏅' },
            top_1: { name: 'الأول في العالم', points: 100, icon: '🌍' }
        };
        
        this.playerAchievements = new Map();
        this.winStreaks = new Map();
        this.killStreaks = new Map();
        logger.info('ELO System initialized');
    }
    
    calculateNewELOs(playerA_ELO, playerB_ELO, playerA_won) {
        const expectedA = 1 / (1 + Math.pow(10, (playerB_ELO - playerA_ELO) / 400));
        const expectedB = 1 / (1 + Math.pow(10, (playerA_ELO - playerB_ELO) / 400));
        
        let kA = this.K_FACTOR, kB = this.K_FACTOR;
        if (playerA_ELO > 2000) kA = 16;
        if (playerB_ELO > 2000) kB = 16;
        if (playerA_ELO < 1000) kA = 40;
        if (playerB_ELO < 1000) kB = 40;
        
        return {
            newELO_A: Math.max(this.MIN_ELO, Math.min(this.MAX_ELO, Math.round(playerA_ELO + kA * (playerA_won - expectedA)))),
            newELO_B: Math.max(this.MIN_ELO, Math.min(this.MAX_ELO, Math.round(playerB_ELO + kB * (1 - playerA_won - expectedB))))
        };
    }
    
    getRank(elo) {
        let currentRank = this.ranks[0];
        for (const rank of this.ranks) {
            if (elo >= rank.minElo) currentRank = rank;
        }
        return currentRank;
    }
    
    getRankProgress(elo) {
        const currentRank = this.getRank(elo);
        const nextRank = this.ranks.find(r => r.minElo > currentRank.minElo);
        if (!nextRank) return { current: currentRank, next: null, progress: 1 };
        return {
            current: currentRank,
            next: nextRank,
            progress: Math.min(1, Math.max(0, (elo - currentRank.minElo) / (nextRank.minElo - currentRank.minElo)))
        };
    }
    
    async checkAchievements(userId, stats) {
        const achieved = [];
        const userAchievements = this.playerAchievements.get(userId) || new Set();
        this.updateStreaks(userId, stats);
        
        const checks = [
            { key: 'first_win', condition: stats.wins === 1 },
            { key: 'win_streak_3', condition: (this.winStreaks.get(userId) || 0) >= 3 },
            { key: 'win_streak_5', condition: (this.winStreaks.get(userId) || 0) >= 5 },
            { key: 'win_streak_10', condition: (this.winStreaks.get(userId) || 0) >= 10 },
            { key: 'kill_streak_5', condition: (this.killStreaks.get(userId) || 0) >= 5 },
            { key: 'kill_streak_10', condition: (this.killStreaks.get(userId) || 0) >= 10 },
            { key: 'perfect_game', condition: stats.kills >= 5 && stats.deaths === 0 },
            { key: 'veteran', condition: stats.gamesPlayed >= 100 },
            { key: 'legend', condition: stats.gamesPlayed >= 500 }
        ];
        
        for (const check of checks) {
            if (check.condition && !userAchievements.has(check.key)) {
                achieved.push(check.key);
                userAchievements.add(check.key);
            }
        }
        
        this.playerAchievements.set(userId, userAchievements);
        return achieved;
    }
    
    updateStreaks(userId, stats) {
        if (stats.won) this.winStreaks.set(userId, (this.winStreaks.get(userId) || 0) + 1);
        else this.winStreaks.set(userId, 0);
        
        if (stats.kills >= 5) this.killStreaks.set(userId, (this.killStreaks.get(userId) || 0) + 1);
        else if (stats.kills === 0) this.killStreaks.set(userId, 0);
    }
    
    getAchievementReward(achievementKey) {
        return this.achievements[achievementKey]?.points || 0;
    }
    
    async processMatch(winnerId, loserId, stats) {
        const winnerResult = await db.query('SELECT elo FROM users WHERE id = $1', [winnerId]);
        const loserResult = await db.query('SELECT elo FROM users WHERE id = $1', [loserId]);
        
        const winnerELO = winnerResult.rows[0]?.elo || this.DEFAULT_ELO;
        const loserELO = loserResult.rows[0]?.elo || this.DEFAULT_ELO;
        
        const { newELO_A: newWinnerELO, newELO_B: newLoserELO } = this.calculateNewELOs(winnerELO, loserELO, true);
        
        await db.transaction(async (client) => {
            await client.query(
                `UPDATE users SET elo = $1, wins = wins + 1, games_played = games_played + 1, total_rewards = total_rewards + $2
                 WHERE id = $3`,
                [newWinnerELO, stats.winReward || 0, winnerId]
            );
            await client.query(
                `UPDATE users SET elo = $1, games_played = games_played + 1 WHERE id = $2`,
                [newLoserELO, loserId]
            );
        });
        
        await cache.delete(winnerId, 'user');
        await cache.delete(loserId, 'user');
        await cache.delete('leaderboard_elo_10', 'leaderboard');
        
        return {
            winner: {
                userId: winnerId,
                oldELO: winnerELO,
                newELO: newWinnerELO,
                change: newWinnerELO - winnerELO,
                rank: this.getRank(newWinnerELO)
            },
            loser: {
                userId: loserId,
                oldELO: loserELO,
                newELO: newLoserELO,
                change: newLoserELO - loserELO,
                rank: this.getRank(newLoserELO)
            }
        };
    }
}

const eloSystem = new ELOSystem();

// ============================================
// 🎯 تحميل إعدادات الخادم
// ============================================
let serverConfig = null;

async function loadServerConfig() {
    try {
        const cached = await cache.get('server_config', 'config');
        if (cached) { serverConfig = cached; return serverConfig; }
        
        const result = await db.query("SELECT value FROM server_config WHERE key = 'server_config'");
        if (result.rows.length === 0) {
            serverConfig = getDefaultConfig();
            await saveServerConfig(serverConfig);
        } else {
            serverConfig = result.rows[0].value;
        }
        
        await cache.set('server_config', serverConfig, 'config');
        return serverConfig;
    } catch (error) {
        logger.error('Error loading config:', { error: error.message });
        return getDefaultConfig();
    }
}

function getDefaultConfig() {
    return {
        rooms: {
            beginner: { enabled: true, maxSeats: 8, seatPrice: 1, rewardMultiplier: 1, maxRooms: 5 },
            advanced: { enabled: true, maxSeats: 12, seatPrice: 5, rewardMultiplier: 2, maxRooms: 3 },
            pro: { enabled: true, maxSeats: 16, seatPrice: 10, rewardMultiplier: 3, maxRooms: 2 }
        },
        game: {
            duration: 300000,
            mapSize: 600,
            boundaryLimit: 280,
            bulletSpeed: 2.8,
            bulletDamage: 25,
            fireCooldown: 2000,
            tankHealth: 100,
            respawnTime: 5000,
            killRewardPercent: 0.5,
            winRewardMultiplier: 2.0,
            minPlayersToStart: 2,
            maxPlayers: 16,
            shieldAmount: 50,
            boostAmount: 100,
            boostRecharge: 0.5
        },
        system: {
            maintenanceMode: false,
            antiCheatEnabled: true,
            adminPassword: process.env.ADMIN_PASSWORD || 'Admin@2024#Battle',
            jwtSecret: process.env.JWT_SECRET || crypto.randomBytes(64).toString('hex'),
            enableAuditLog: true,
            rateLimitWindow: 60000,
            rateLimitMax: 100
        },
        appearance: {
            gameLogo: '/images/default/logo.png',
            backgroundImage: '/images/default/background.jpg',
            primaryColor: '#d4af37',
            secondaryColor: '#0a0f1a',
            accentColor: '#ff6b6b'
        },
        monitoring: {
            enablePrometheus: true,
            enableDetailedLogging: true,
            healthCheckInterval: 15000,
            slowQueryThreshold: 1000
        },
        features: {
            enableRespawn: true,
            enableBoost: true,
            enableShield: true,
            enableKillFeed: true
        }
    };
}

async function saveServerConfig(config) {
    try {
        await db.query(
            `INSERT INTO server_config (key, value, updated_at) VALUES ('server_config', $1, CURRENT_TIMESTAMP)
             ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = CURRENT_TIMESTAMP`,
            [config]
        );
        await cache.set('server_config', config, 'config');
        serverConfig = config;
        logger.info('Server config saved');
        return true;
    } catch (error) {
        logger.error('Error saving config:', { error: error.message });
        return false;
    }
}

// ============================================
// 🎮 إدارة المستخدمين
// ============================================
class UserManager {
    constructor() {
        this.onlineUsers = new Map();
        logger.info('User Manager initialized');
    }
    
    async getUser(userId) {
        try {
            const cached = await cache.get(`user_${userId}`, 'user');
            if (cached) return cached;
            
            const result = await db.query(
                `SELECT id, username, balance, elo, kills, wins, games_played, total_rewards,
                        is_admin, is_banned, ban_reason, banned_until
                 FROM users WHERE id = $1`,
                [userId]
            );
            
            if (result.rows.length === 0) {
                const newUser = {
                    id: userId,
                    username: `لاعب_${userId.slice(0, 6)}`,
                    balance: 100,
                    elo: 1000,
                    kills: 0,
                    wins: 0,
                    games_played: 0,
                    total_rewards: 0,
                    is_admin: false,
                    is_banned: false
                };
                
                await db.query(
                    `INSERT INTO users (id, username, balance, elo, is_admin, created_at)
                     VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP)`,
                    [newUser.id, newUser.username, newUser.balance, newUser.elo, newUser.is_admin]
                );
                
                await cache.set(`user_${userId}`, newUser, 'user');
                return newUser;
            }
            
            const user = result.rows[0];
            await cache.set(`user_${userId}`, user, 'user');
            return user;
        } catch (error) {
            logger.error('Error getting user:', { error: error.message, userId });
            return null;
        }
    }
    
    async updateUser(userId, data) {
        try {
            const fields = [];
            const values = [];
            let i = 1;
            for (const [key, value] of Object.entries(data)) {
                if (key !== 'id' && key !== 'created_at') {
                    fields.push(`${key} = $${i}`);
                    values.push(value);
                    i++;
                }
            }
            values.push(userId);
            
            await db.query(
                `UPDATE users SET ${fields.join(', ')}, updated_at = CURRENT_TIMESTAMP WHERE id = $${i}`,
                values
            );
            
            await cache.delete(`user_${userId}`, 'user');
            return true;
        } catch (error) {
            logger.error('Error updating user:', { error: error.message, userId });
            return false;
        }
    }
    
    async addBalance(userId, amount, description = '') {
        try {
            const user = await this.getUser(userId);
            if (!user) return false;
            
            const newBalance = user.balance + amount;
            if (newBalance < 0) return false;
            
            await db.transaction(async (client) => {
                await client.query(
                    `UPDATE users SET balance = $1 WHERE id = $2`,
                    [newBalance, userId]
                );
                await client.query(
                    `INSERT INTO transactions (id, user_id, type, amount, balance_before, balance_after, description)
                     VALUES ($1, $2, $3, $4, $5, $6, $7)`,
                    [
                        `tx_${Date.now()}_${uuidv4().slice(0, 8)}`,
                        userId,
                        amount > 0 ? 'credit' : 'debit',
                        Math.abs(amount),
                        user.balance,
                        newBalance,
                        description
                    ]
                );
            });
            
            await cache.delete(`user_${userId}`, 'user');
            return newBalance;
        } catch (error) {
            logger.error('Error adding balance:', { error: error.message, userId });
            return false;
        }
    }
    
    async getLeaderboard(type = 'elo', limit = 100) {
        const cacheKey = `leaderboard_${type}_${limit}`;
        const cached = await cache.get(cacheKey, 'leaderboard');
        if (cached) return cached;
        
        let orderBy = 'elo DESC';
        if (type === 'wins') orderBy = 'wins DESC, elo DESC';
        if (type === 'kills') orderBy = 'kills DESC, elo DESC';
        if (type === 'rewards') orderBy = 'total_rewards DESC, elo DESC';
        
        const result = await db.query(
            `SELECT id, username, elo, kills, wins, games_played, balance, total_rewards
             FROM users WHERE is_banned = false
             ORDER BY ${orderBy} LIMIT $1`,
            [limit]
        );
        
        const leaderboard = result.rows.map((user, index) => ({
            ...user,
            rank: index + 1,
            rankName: eloSystem.getRank(user.elo || 1000).name
        }));
        
        await cache.set(cacheKey, leaderboard, 'leaderboard');
        return leaderboard;
    }
    
    setOnline(userId, socketId) {
        this.onlineUsers.set(userId, { socketId, connectedAt: Date.now() });
    }
    
    setOffline(userId) {
        this.onlineUsers.delete(userId);
    }
    
    getOnline(userId) {
        return this.onlineUsers.get(userId);
    }
    
    getOnlineCount() {
        return this.onlineUsers.size;
    }
    
    getOnlineUsers() {
        return Array.from(this.onlineUsers.entries()).map(([id, data]) => ({ userId: id, ...data }));
    }
}

const userManager = new UserManager();

// ============================================
// 🎮 إدارة الغرف
// ============================================
class RoomManager {
    constructor() {
        this.rooms = new Map();
        this.activeGames = new Map();
        this.pendingReconnects = new Map();
        this.lock = new DistributedLock();
        this.roomTypes = {
            beginner: { name: 'غرفة المبتدئين', icon: '🟢', minElo: 0, maxElo: 1200 },
            advanced: { name: 'غرفة المتقدمين', icon: '🟡', minElo: 1000, maxElo: 1800 },
            pro: { name: 'غرفة المحترفين', icon: '🔴', minElo: 1600, maxElo: 3000 }
        };
        
        this.initializeRooms();
        setInterval(() => this.cleanupInactiveRooms(), 60000);
        logger.info('Room Manager initialized');
    }
    
    async initializeRooms() {
        try {
            const config = await loadServerConfig();
            const roomConfigs = config.rooms || {};
            
            for (const [type, settings] of Object.entries(roomConfigs)) {
                if (!settings.enabled) continue;
                
                for (let i = 1; i <= settings.maxRooms; i++) {
                    const roomId = `${type}_${i}`;
                    if (!this.rooms.has(roomId)) {
                        this.rooms.set(roomId, {
                            id: roomId,
                            type: type,
                            name: `${this.roomTypes[type]?.icon || '🏠'} ${this.roomTypes[type]?.name || type} ${i}`,
                            maxSeats: settings.maxSeats,
                            seatPrice: settings.seatPrice,
                            rewardMultiplier: settings.rewardMultiplier || 1,
                            players: [],
                            status: 'waiting',
                            createdAt: Date.now(),
                            gameRound: 0,
                            minElo: this.roomTypes[type]?.minElo || 0,
                            maxElo: this.roomTypes[type]?.maxElo || 3000,
                            stats: { totalGames: 0, totalKills: 0 }
                        });
                    }
                }
            }
            
            logger.info(`Rooms initialized: ${this.rooms.size} rooms`);
            this.broadcastRoomsList();
        } catch (error) {
            logger.error('Error initializing rooms:', { error: error.message });
        }
    }
    
    async joinRoom(socket, userId, roomId) {
        const lockKey = `join_${userId}_${roomId}`;
        try {
            await this.lock.acquireLock(lockKey, userId, 5000);
            
            const room = this.rooms.get(roomId);
            if (!room) {
                socket.emit('error', { message: 'الغرفة غير موجودة' });
                return null;
            }
            
            if (room.status !== 'waiting') {
                socket.emit('error', { message: 'الغرفة مشغولة' });
                return null;
            }
            
            if (room.players.length >= room.maxSeats) {
                socket.emit('error', { message: 'الغرفة ممتلئة' });
                return null;
            }
            
            const user = await userManager.getUser(userId);
            if (!user) {
                socket.emit('error', { message: 'بيانات المستخدم غير موجودة' });
                return null;
            }
            
            if (user.is_banned) {
                socket.emit('error', { message: 'أنت محظور' });
                return null;
            }
            
            if (user.elo < room.minElo || user.elo > room.maxElo) {
                socket.emit('error', { 
                    message: `تصنيفك ${user.elo} غير مناسب (${room.minElo}-${room.maxElo})` 
                });
                return null;
            }
            
            if (user.balance < room.seatPrice) {
                socket.emit('error', { 
                    message: `رصيد غير كافٍ. السعر: ${room.seatPrice}$` 
                });
                return null;
            }
            
            const newBalance = await userManager.addBalance(
                userId, 
                -room.seatPrice, 
                `انضمام للغرفة ${room.name}`
            );
            
            if (newBalance === false) {
                socket.emit('error', { message: 'فشل خصم الرصيد' });
                return null;
            }
            
            const player = {
                userId,
                socketId: socket.id,
                username: user.username,
                elo: user.elo,
                balance: newBalance,
                health: 100,
                kills: 0,
                joinedAt: Date.now(),
                position: null
            };
            
            room.players.push(player);
            socket.join(roomId);
            userManager.setOnline(userId, socket.id);
            
            socket.emit('room_joined', {
                roomId,
                roomName: room.name,
                players: room.players.map(p => ({
                    userId: p.userId,
                    username: p.username,
                    elo: p.elo
                })),
                balance: newBalance,
                seatPrice: room.seatPrice,
                maxSeats: room.maxSeats
            });
            
            io.to(roomId).emit('player_joined', {
                userId,
                username: player.username,
                elo: player.elo,
                playersCount: room.players.length,
                maxSeats: room.maxSeats
            });
            
            this.broadcastRoomsList();
            
            if (room.players.length >= 2) {
                await this.startGame(roomId);
            }
            
            this.lock.releaseLock(lockKey, userId);
            return room;
        } catch (error) {
            logger.error('Error joining room:', { error: error.message, userId, roomId });
            socket.emit('error', { message: error.message });
            await this.lock.releaseLock(lockKey, userId);
            return null;
        }
    }
    
    async leaveRoom(socket, userId, roomId) {
        const lockKey = `leave_${userId}_${roomId}`;
        try {
            await this.lock.acquireLock(lockKey, userId, 5000);
            
            const room = this.rooms.get(roomId);
            if (!room) { await this.lock.releaseLock(lockKey, userId); return; }
            
            const playerIndex = room.players.findIndex(p => p.userId === userId);
            if (playerIndex === -1) { await this.lock.releaseLock(lockKey, userId); return; }
            
            const player = room.players[playerIndex];
            
            if (room.status === 'waiting') {
                await userManager.addBalance(
                    userId, 
                    room.seatPrice, 
                    `استرداد من غرفة ${room.name}`
                );
                socket.emit('balance_update', { 
                    balance: player.balance + room.seatPrice,
                    message: `تم إعادة ${room.seatPrice}$` 
                });
            }
            
            room.players.splice(playerIndex, 1);
            socket.leave(roomId);
            userManager.setOffline(userId);
            
            io.to(roomId).emit('player_left', {
                userId,
                playersCount: room.players.length,
                maxSeats: room.maxSeats
            });
            
            if (room.players.length === 0 && room.status === 'waiting') {
                this.resetRoom(roomId);
            }
            
            this.broadcastRoomsList();
            await this.lock.releaseLock(lockKey, userId);
        } catch (error) {
            logger.error('Error leaving room:', { error: error.message, userId, roomId });
            await this.lock.releaseLock(lockKey, userId);
        }
    }
    
    async startGame(roomId) {
        const lockKey = `start_${roomId}`;
        try {
            await this.lock.acquireLock(lockKey, 'system', 10000);
            
            const room = this.rooms.get(roomId);
            if (!room || room.status !== 'waiting' || room.players.length < 2) {
                await this.lock.releaseLock(lockKey, 'system');
                return null;
            }
            
            room.status = 'active';
            room.startTime = Date.now();
            
            const game = new GameEngine(roomId, room);
            game.start();
            this.activeGames.set(roomId, game);
            
            const gameData = {
                roomId,
                players: room.players.map(p => ({
                    userId: p.userId,
                    username: p.username,
                    elo: p.elo
                })),
                startTime: room.startTime,
                gameRound: room.gameRound + 1,
                totalPlayers: room.players.length
            };
            
            for (const player of room.players) {
                const socket = io.sockets.sockets.get(player.socketId);
                if (socket) {
                    socket.emit('game_start', {
                        ...gameData,
                        yourId: player.userId,
                        position: this.getSpawnPosition(room.players.indexOf(player))
                    });
                }
            }
            
            room.gameRound++;
            logger.info(`Game started in ${room.name} with ${room.players.length} players`);
            monitoring.recordGameStarted(room.players.length);
            
            this.broadcastRoomsList();
            await this.lock.releaseLock(lockKey, 'system');
            return game;
        } catch (error) {
            logger.error('Error starting game:', { error: error.message, roomId });
            await this.lock.releaseLock(lockKey, 'system');
            return null;
        }
    }
    
    getSpawnPosition(index) {
        const positions = [
            { x: -120, z: -80 }, { x: 120, z: 80 },
            { x: -100, z: 60 }, { x: 100, z: -60 },
            { x: -50, z: -100 }, { x: 50, z: 100 },
            { x: -80, z: -120 }, { x: 80, z: 120 },
            { x: 0, z: -100 }, { x: 0, z: 100 },
            { x: -100, z: 0 }, { x: 100, z: 0 }
        ];
        return positions[index % positions.length] || positions[0];
    }
    
    async resetRoom(roomId) {
        const lockKey = `reset_${roomId}`;
        try {
            await this.lock.acquireLock(lockKey, 'system', 10000);
            
            const oldRoom = this.rooms.get(roomId);
            if (!oldRoom) { await this.lock.releaseLock(lockKey, 'system'); return; }
            
            if (this.activeGames.has(roomId)) {
                this.activeGames.get(roomId).stop();
                this.activeGames.delete(roomId);
            }
            
            const newRoom = {
                ...oldRoom,
                players: [],
                status: 'waiting',
                createdAt: Date.now(),
                startTime: null
            };
            
            this.rooms.set(roomId, newRoom);
            logger.info(`Room reset: ${oldRoom.name}`);
            this.broadcastRoomsList();
            
            await this.lock.releaseLock(lockKey, 'system');
        } catch (error) {
            logger.error('Error resetting room:', { error: error.message, roomId });
            await this.lock.releaseLock(lockKey, 'system');
        }
    }
    
    broadcastRoomsList() {
        const roomsList = [];
        for (const [roomId, room] of this.rooms) {
            if (room.status === 'waiting' || room.status === 'active') {
                roomsList.push({
                    id: roomId,
                    name: room.name,
                    type: room.type,
                    players: room.players.length,
                    maxSeats: room.maxSeats,
                    seatPrice: room.seatPrice,
                    status: room.status,
                    minElo: room.minElo,
                    maxElo: room.maxElo,
                    gameRound: room.gameRound
                });
            }
        }
        io.emit('rooms_list', { rooms: roomsList });
    }
    
    broadcastLobbyStats() {
        io.emit('lobby_stats', {
            totalPlayers: userManager.getOnlineCount(),
            activeRooms: Array.from(this.rooms.values()).filter(r => r.status === 'active').length,
            waitingRooms: Array.from(this.rooms.values()).filter(r => r.status === 'waiting').length,
            totalRooms: this.rooms.size,
            activeGames: this.activeGames.size,
            serverTime: Date.now(),
            version: '12.0.0'
        });
    }
    
    cleanupInactiveRooms() {
        const now = Date.now();
        const timeout = 3600000;
        for (const [roomId, room] of this.rooms) {
            if (room.status === 'waiting' && room.players.length === 0 && now - room.createdAt > timeout) {
                this.rooms.delete(roomId);
                logger.info(`Removed inactive room: ${roomId}`);
            }
        }
    }
}

const roomManager = new RoomManager();

// ============================================
// 🎮 محرك اللعبة المتقدم
// ============================================
class GameEngine {
    constructor(roomId, room) {
        this.roomId = roomId;
        this.room = room;
        this.tanks = new Map();
        this.bullets = [];
        this.obstacles = [];
        this.powerups = [];
        this.killFeed = [];
        this.aliveCount = 0;
        this.gameEnded = false;
        this.gameStartTime = Date.now();
        this.tickInterval = null;
        this.bulletId = 0;
        this.killRewards = new Map();
        this.eliminatedPlayers = new Set();
        this.gameRound = (room.gameRound || 0) + 1;
        this.config = null;
        this.zoneShrink = {
            active: false,
            currentRadius: 280,
            targetRadius: 50,
            shrinkRate: 0,
            startTime: 0,
            duration: 0
        };
        
        this.initGame();
        logger.info(`GameEngine created for ${roomId}`);
    }
    
    async initGame() {
        this.config = await loadServerConfig();
        const gameConfig = this.config.game || {};
        
        const spawnPositions = this.generateSpawnPositions();
        
        for (let i = 0; i < this.room.players.length; i++) {
            const player = this.room.players[i];
            const position = spawnPositions[i % spawnPositions.length];
            
            this.tanks.set(player.userId, {
                position: { ...position },
                health: gameConfig.tankHealth || 100,
                shield: gameConfig.shieldAmount || 50,
                maxShield: gameConfig.shieldAmount || 50,
                boost: gameConfig.boostAmount || 100,
                maxBoost: gameConfig.boostAmount || 100,
                rotation: 0,
                respawning: false,
                name: player.username || 'لاعب',
                kills: 0,
                lastShoot: 0,
                speed: 0.25,
                rotationSpeed: 0.04,
                active: true,
                powerups: []
            });
        }
        
        this.obstacles = this.generateObstacles();
        this.powerups = this.generatePowerups();
        this.aliveCount = this.tanks.size;
        this.startZoneShrink();
    }
    
    startZoneShrink() {
        const gameConfig = this.config.game || {};
        const duration = gameConfig.duration || 300000;
        
        this.zoneShrink.active = true;
        this.zoneShrink.currentRadius = gameConfig.boundaryLimit || 280;
        this.zoneShrink.targetRadius = 50;
        this.zoneShrink.shrinkRate = (this.zoneShrink.currentRadius - this.zoneShrink.targetRadius) / (duration / 1000);
        this.zoneShrink.startTime = Date.now();
        this.zoneShrink.duration = duration;
    }
    
    generateSpawnPositions() {
        const positions = [];
        const count = this.room.players.length;
        const radius = 300;
        for (let i = 0; i < count; i++) {
            const angle = (i / count) * 2 * Math.PI + Math.random() * 0.1;
            const distance = radius * 0.4 + Math.random() * radius * 0.6;
            positions.push({
                x: Math.cos(angle) * distance,
                z: Math.sin(angle) * distance
            });
        }
        return positions;
    }
    
    generateObstacles() {
        const obstacles = [];
        const count = 30 + Math.floor(Math.random() * 20);
        for (let i = 0; i < count; i++) {
            const angle = Math.random() * 2 * Math.PI;
            const distance = 50 + Math.random() * 250;
            const size = 1.5 + Math.random() * 4;
            obstacles.push({
                id: `obs_${i}`,
                position: { x: Math.cos(angle) * distance, z: Math.sin(angle) * distance },
                radius: size,
                health: 50 + Math.random() * 50,
                destroyed: false
            });
        }
        return obstacles;
    }
    
    generatePowerups() {
        const powerups = [];
        const types = ['health', 'shield', 'boost', 'speed', 'damage'];
        const count = 10 + Math.floor(Math.random() * 10);
        for (let i = 0; i < count; i++) {
            const angle = Math.random() * 2 * Math.PI;
            const distance = 50 + Math.random() * 230;
            powerups.push({
                id: `pw_${i}`,
                type: types[Math.floor(Math.random() * types.length)],
                position: { x: Math.cos(angle) * distance, z: Math.sin(angle) * distance },
                active: true,
                respawnTime: 10000,
                lastSpawn: Date.now()
            });
        }
        return powerups;
    }
    
    start() {
        this.tickInterval = setInterval(() => {
            const timeStep = 0.05;
            this.update(timeStep);
        }, 50);
        
        setInterval(() => {
            io.to(this.roomId).emit('game_ping', { time: Date.now() });
        }, 5000);
        
        logger.info(`Game started for ${this.roomId}`);
    }
    
    update(timeStep) {
        if (this.gameEnded) return;
        
        this.updateBullets(timeStep);
        this.updatePowerups(timeStep);
        this.updateZoneShrink(timeStep);
        this.updateTanks(timeStep);
        this.checkCollisions();
        this.updateAliveCount();
        this.broadcastGameState();
        this.checkGameEnd();
    }
    
    updateBullets(timeStep) {
        const toRemove = [];
        const boundary = this.zoneShrink.currentRadius || 280;
        
        for (let i = 0; i < this.bullets.length; i++) {
            const bullet = this.bullets[i];
            bullet.position.x += bullet.velocity.x * timeStep;
            bullet.position.z += bullet.velocity.z * timeStep;
            bullet.life--;
            
            if (bullet.life <= 0 || Math.abs(bullet.position.x) > boundary || Math.abs(bullet.position.z) > boundary) {
                toRemove.push(i);
            }
        }
        
        for (const idx of toRemove.sort((a, b) => b - a)) {
            this.bullets.splice(idx, 1);
        }
    }
    
    updatePowerups(timeStep) {
        const now = Date.now();
        for (const powerup of this.powerups) {
            if (!powerup.active && now - powerup.lastSpawn > powerup.respawnTime) {
                powerup.active = true;
                const angle = Math.random() * 2 * Math.PI;
                const distance = 50 + Math.random() * 200;
                powerup.position = { x: Math.cos(angle) * distance, z: Math.sin(angle) * distance };
            }
        }
    }
    
    updateZoneShrink(timeStep) {
        if (!this.zoneShrink.active) return;
        
        const elapsed = (Date.now() - this.zoneShrink.startTime) / 1000;
        const progress = Math.min(1, elapsed / (this.zoneShrink.duration / 1000));
        
        this.zoneShrink.currentRadius -= this.zoneShrink.shrinkRate * timeStep;
        this.zoneShrink.currentRadius = Math.max(this.zoneShrink.targetRadius, this.zoneShrink.currentRadius);
        
        if (Math.floor(progress * 10) % 2 === 0) {
            io.to(this.roomId).emit('zone_update', {
                radius: this.zoneShrink.currentRadius,
                progress: progress,
                damage: (1 - progress) * 2
            });
        }
    }
    
    updateTanks(timeStep) {
        for (const [userId, tank] of this.tanks) {
            if (tank.shield < tank.maxShield) {
                tank.shield = Math.min(tank.maxShield, tank.shield + 1 * timeStep);
            }
            if (tank.boost < tank.maxBoost) {
                tank.boost = Math.min(tank.maxBoost, tank.boost + 5 * timeStep);
            }
            
            const distance = Math.sqrt(tank.position.x * tank.position.x + tank.position.z * tank.position.z);
            if (distance > this.zoneShrink.currentRadius) {
                const damage = (1 - this.zoneShrink.currentRadius / 280) * 2 * timeStep;
                this.handleDamage(null, userId, damage);
            }
            
            if (tank.health <= 0 && !tank.respawning && !this.eliminatedPlayers.has(userId)) {
                this.handleTankDestroyed(userId);
            }
        }
    }
    
    checkCollisions() {
        const toRemove = [];
        
        for (let i = 0; i < this.bullets.length; i++) {
            const bullet = this.bullets[i];
            let bulletHit = false;
            
            for (const [userId, tank] of this.tanks) {
                if (userId === bullet.ownerId) continue;
                if (tank.health <= 0) continue;
                
                const dx = bullet.position.x - tank.position.x;
                const dz = bullet.position.z - tank.position.z;
                const distance = Math.sqrt(dx * dx + dz * dz);
                
                if (distance < 2.5) {
                    this.handleDamage(bullet.ownerId, userId, bullet.damage || 25);
                    bulletHit = true;
                    break;
                }
            }
            
            if (!bulletHit) {
                for (const obstacle of this.obstacles) {
                    if (obstacle.destroyed) continue;
                    const dx = bullet.position.x - obstacle.position.x;
                    const dz = bullet.position.z - obstacle.position.z;
                    const distance = Math.sqrt(dx * dx + dz * dz);
                    
                    if (distance < obstacle.radius) {
                        obstacle.health -= 25;
                        if (obstacle.health <= 0) {
                            obstacle.destroyed = true;
                            bulletHit = true;
                            io.to(this.roomId).emit('obstacle_destroyed', {
                                id: obstacle.id,
                                position: obstacle.position
                            });
                        }
                        break;
                    }
                }
            }
            
            if (!bulletHit) {
                for (const powerup of this.powerups) {
                    if (!powerup.active) continue;
                    const dx = bullet.position.x - powerup.position.x;
                    const dz = bullet.position.z - powerup.position.z;
                    const distance = Math.sqrt(dx * dx + dz * dz);
                    
                    if (distance < 1.5) {
                        this.collectPowerup(bullet.ownerId, powerup);
                        bulletHit = true;
                        break;
                    }
                }
            }
            
            if (bulletHit) toRemove.push(i);
        }
        
        for (const idx of toRemove.sort((a, b) => b - a)) {
            this.bullets.splice(idx, 1);
        }
    }
    
    handleDamage(shooterId, targetId, damage) {
        const target = this.tanks.get(targetId);
        if (!target || target.health <= 0 || this.eliminatedPlayers.has(targetId)) return;
        
        let actualDamage = damage;
        if (target.shield > 0) {
            const shieldDamage = Math.min(target.shield, actualDamage);
            target.shield -= shieldDamage;
            actualDamage -= shieldDamage;
        }
        
        const newHealth = Math.max(0, target.health - actualDamage);
        target.health = newHealth;
        
        io.to(this.roomId).emit('player_hit', {
            targetId,
            shooterId,
            damage: actualDamage,
            newHealth,
            shield: target.shield,
            timestamp: Date.now()
        });
        
        if (newHealth <= 0) {
            this.handleTankDestroyed(targetId, shooterId);
        }
    }
    
    handleTankDestroyed(targetId, shooterId = null) {
        const target = this.tanks.get(targetId);
        if (!target || this.eliminatedPlayers.has(targetId)) return;
        
        this.eliminatedPlayers.add(targetId);
        target.health = 0;
        target.active = false;
        
        if (shooterId && shooterId !== targetId) {
            const reward = this.calculateKillReward(shooterId);
            this.killRewards.set(shooterId, (this.killRewards.get(shooterId) || 0) + reward);
            
            const killer = this.tanks.get(shooterId);
            if (killer) killer.kills = (killer.kills || 0) + 1;
            
            io.to(this.roomId).emit('player_eliminated', {
                targetId,
                killerId: shooterId,
                reward,
                targetName: target.name,
                killerName: this.tanks.get(shooterId)?.name || 'لاعب',
                aliveCount: this.aliveCount - 1
            });
            
            this.killFeed.push({
                killer: this.tanks.get(shooterId)?.name || 'لاعب',
                target: target.name,
                timestamp: Date.now()
            });
            if (this.killFeed.length > 20) this.killFeed.shift();
            
            io.to(this.roomId).emit('kill_feed_update', { kills: this.killFeed.slice(-10) });
        }
        
        const respawnTime = this.config?.game?.respawnTime || 5000;
        setTimeout(() => this.respawnPlayer(targetId), respawnTime);
    }
    
    respawnPlayer(userId) {
        if (this.gameEnded) return;
        
        const tank = this.tanks.get(userId);
        if (!tank) return;
        
        if (this.eliminatedPlayers.has(userId)) {
            this.eliminatedPlayers.delete(userId);
            tank.health = 100;
            tank.shield = 50;
            tank.boost = 100;
            tank.active = true;
            tank.respawning = false;
            
            const positions = this.generateSpawnPositions();
            const pos = positions[Math.floor(Math.random() * positions.length)];
            tank.position = { ...pos };
            
            io.to(this.roomId).emit('player_respawned', {
                userId,
                position: tank.position,
                health: 100,
                shield: 50
            });
            
            io.to(userId).emit('respawn_success', {
                message: '✅ تم إعادة إحياء دبابتك!'
            });
        }
    }
    
    collectPowerup(userId, powerup) {
        const tank = this.tanks.get(userId);
        if (!tank) return;
        
        powerup.active = false;
        powerup.lastSpawn = Date.now();
        
        let message = '';
        switch(powerup.type) {
            case 'health':
                const heal = 50;
                tank.health = Math.min(100, tank.health + heal);
                message = `❤️ +${heal} صحة`;
                break;
            case 'shield':
                tank.shield = Math.min(tank.maxShield, tank.shield + 30);
                message = `🛡️ +30 درع`;
                break;
            case 'boost':
                tank.boost = Math.min(tank.maxBoost, tank.boost + 50);
                message = `⚡ +50 تعزيز`;
                break;
            case 'speed':
                tank.speed *= 1.3;
                setTimeout(() => { tank.speed /= 1.3; }, 5000);
                message = `💨 زيادة السرعة`;
                break;
            case 'damage':
                message = `💥 زيادة الضرر`;
                break;
        }
        
        io.to(userId).emit('powerup_collected', { type: powerup.type, message });
        io.to(this.roomId).emit('powerup_used', { userId, type: powerup.type, position: powerup.position });
    }
    
    updateAliveCount() {
        let alive = 0;
        for (const [userId, tank] of this.tanks) {
            if (tank.health > 0 && !this.eliminatedPlayers.has(userId)) alive++;
        }
        this.aliveCount = alive;
    }
    
    broadcastGameState() {
        const state = {
            tanks: Array.from(this.tanks.entries()).map(([userId, tank]) => ({
                userId,
                position: tank.position,
                health: tank.health,
                shield: tank.shield,
                rotation: tank.rotation,
                active: tank.active
            })),
            bullets: this.bullets.map(b => ({ id: b.id, position: b.position, ownerId: b.ownerId })),
            obstacles: this.obstacles.filter(o => !o.destroyed),
            powerups: this.powerups.filter(p => p.active),
            aliveCount: this.aliveCount,
            zoneRadius: this.zoneShrink.currentRadius,
            timestamp: Date.now()
        };
        io.to(this.roomId).emit('game_state', state);
    }
    
    checkGameEnd() {
        const gameDuration = this.config?.game?.duration || 300000;
        const timeElapsed = Date.now() - this.gameStartTime;
        
        if (this.aliveCount <= 1 || timeElapsed >= gameDuration) {
            this.endGame();
        }
    }
    
    async endGame() {
        if (this.gameEnded) return;
        this.gameEnded = true;
        this.stop();
        
        let winnerId = null;
        for (const [userId, tank] of this.tanks) {
            if (tank.health > 0 && !this.eliminatedPlayers.has(userId)) {
                winnerId = userId;
                break;
            }
        }
        
        const duration = Math.floor((Date.now() - this.gameStartTime) / 1000);
        const winnerReward = winnerId ? this.calculateWinReward(winnerId) : 0;
        
        try {
            await db.query(
                `INSERT INTO matches (id, room_id, winner_id, players, kill_feed, start_time, end_time, duration, total_players, game_round)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
                [
                    `match_${Date.now()}_${uuidv4().slice(0, 8)}`,
                    this.roomId,
                    winnerId,
                    JSON.stringify(this.room.players.map(p => ({
                        userId: p.userId,
                        username: p.username,
                        kills: this.tanks.get(p.userId)?.kills || 0
                    }))),
                    JSON.stringify(this.killFeed.slice(-20)),
                    new Date(this.gameStartTime),
                    new Date(),
                    duration,
                    this.tanks.size,
                    this.gameRound
                ]
            );
        } catch (error) {
            logger.error('Error saving match:', { error: error.message });
        }
        
        if (winnerId) await this.distributeRewards(winnerId, winnerReward);
        
        io.to(this.roomId).emit('game_ended', {
            winner: winnerId,
            winReward: winnerReward,
            duration,
            totalPlayers: this.tanks.size,
            killFeed: this.killFeed.slice(-10),
            kills: Array.from(this.killRewards.entries()).map(([id, reward]) => ({
                userId: id,
                kills: this.tanks.get(id)?.kills || 0,
                reward
            })),
            gameRound: this.gameRound,
            timestamp: Date.now()
        });
        
        this.room.status = 'finished';
        logger.info(`Game ended in ${this.room.name}. Winner: ${winnerId || 'none'}`);
        monitoring.recordGameEnded(duration);
        
        setTimeout(() => roomManager.resetRoom(this.roomId), 10000);
    }
    
    calculateKillReward(killerId) {
        const killer = this.room.players.find(p => p.userId === killerId);
        if (!killer) return 0;
        const seatPrice = this.room.seatPrice || 1;
        const percent = this.config?.game?.killRewardPercent || 0.5;
        return Math.round(seatPrice * percent * 100) / 100;
    }
    
    calculateWinReward(winnerId) {
        const winner = this.room.players.find(p => p.userId === winnerId);
        if (!winner) return 0;
        const seatPrice = this.room.seatPrice || 1;
        const multiplier = this.config?.game?.winRewardMultiplier || 2.0;
        const kills = this.tanks.get(winnerId)?.kills || 0;
        const killBonus = kills * seatPrice * (this.config?.game?.killRewardPercent || 0.5);
        return Math.round((seatPrice * multiplier + killBonus) * 100) / 100;
    }
    
    async distributeRewards(winnerId, winReward) {
        try {
            await db.transaction(async (client) => {
                for (const player of this.room.players) {
                    const isWinner = player.userId === winnerId;
                    const kills = this.tanks.get(player.userId)?.kills || 0;
                    const reward = isWinner ? winReward : 0;
                    const killsReward = kills * 0.1;
                    const totalReward = reward + killsReward;
                    
                    const result = await client.query('SELECT elo, balance FROM users WHERE id = $1', [player.userId]);
                    const currentELO = result.rows[0]?.elo || 1000;
                    const currentBalance = result.rows[0]?.balance || 0;
                    
                    let eloChange = isWinner ? 15 + Math.floor(kills / 2) : -5 + kills * 2;
                    const newELO = Math.max(1, currentELO + eloChange);
                    const newBalance = currentBalance + totalReward;
                    
                    await client.query(
                        `UPDATE users SET balance = $1, elo = $2, games_played = games_played + 1,
                         wins = wins + $3, kills = kills + $4, total_rewards = total_rewards + $5
                         WHERE id = $6`,
                        [newBalance, newELO, isWinner ? 1 : 0, kills, totalReward, player.userId]
                    );
                    
                    const rank = eloSystem.getRank(newELO);
                    io.to(player.socketId).emit('game_ended_rewards', {
                        isWinner,
                        reward: totalReward,
                        eloChange,
                        newELO,
                        newBalance,
                        rank,
                        kills,
                        killsReward
                    });
                }
            });
        } catch (error) {
            logger.error('Error distributing rewards:', { error: error.message });
            monitoring.recordError('reward_distribution_error', error.message, 'game');
        }
    }
    
    stop() {
        if (this.tickInterval) {
            clearInterval(this.tickInterval);
            this.tickInterval = null;
        }
    }
}

// ============================================
// 🚀 تهيئة Express
// ============================================
const app = express();
const server = http.createServer(app);

app.use(helmet({
    contentSecurityPolicy: {
        directives: {
            defaultSrc: ["'self'"],
            styleSrc: ["'self'", "'unsafe-inline'"],
            scriptSrc: ["'self'", "'unsafe-inline'"],
            imgSrc: ["'self'", "data:", "https:"],
            connectSrc: ["'self'", "wss:", "ws:"]
        }
    },
    hsts: { maxAge: 31536000, includeSubDomains: true, preload: true }
}));

app.use(compression());
app.use(cors({
    origin: process.env.CORS_ORIGIN?.split(',') || '*',
    methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
    credentials: true,
    allowedHeaders: ['Content-Type', 'Authorization', 'X-Admin-Token', 'X-Request-ID']
}));

app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true, limit: '10mb' }));

app.use((req, res, next) => {
    req.requestId = req.headers['x-request-id'] || uuidv4();
    res.setHeader('X-Request-ID', req.requestId);
    next();
});

app.use(morgan('combined', {
    stream: { write: (message) => logger.info('HTTP Request', { message: message.trim() }) }
}));

const limiter = rateLimit({
    windowMs: 60000,
    max: 100,
    message: { error: 'Too many requests, please try again later.' }
});
app.use('/api/', limiter);

// ============================================
// 📊 نقاط النهاية العامة
// ============================================
app.get('/health', (req, res) => {
    const health = {
        status: 'ok',
        timestamp: new Date().toISOString(),
        uptime: monitoring.formatUptime(Math.floor((Date.now() - monitoring.startTime) / 1000)),
        version: '12.0.0',
        service: 'battle-tanks-royale',
        requestId: req.requestId,
        checks: {
            database: db.getHealth(),
            connections: monitoring.metrics.connections.active,
            memory: process.memoryUsage().heapUsed / 1024 / 1024,
            cpu: process.cpuUsage().user / 1000000
        }
    };
    res.status(health.checks.database.connected ? 200 : 503).json(health);
});

app.get('/metrics', (req, res) => {
    const stats = monitoring.getStats();
    let output = '# HELP battle_tanks_metrics Metrics for Battle Tanks Royale\n';
    output += '# TYPE battle_tanks_metrics gauge\n';
    output += `battle_tanks_connections_active ${stats.connections.active}\n`;
    output += `battle_tanks_connections_total ${stats.connections.total}\n`;
    output += `battle_tanks_requests_total ${stats.requests.total}\n`;
    output += `battle_tanks_requests_success ${stats.requests.success}\n`;
    output += `battle_tanks_requests_error ${stats.requests.error}\n`;
    output += `battle_tanks_games_active ${stats.games.active}\n`;
    output += `battle_tanks_games_total ${stats.games.total}\n`;
    output += `battle_tanks_errors_total ${stats.errors.total}\n`;
    output += `battle_tanks_database_connected ${stats.database.connected ? 1 : 0}\n`;
    res.set('Content-Type', 'text/plain').send(output);
});

app.get('/api/config', async (req, res) => {
    try {
        const config = await loadServerConfig();
        const sanitized = { ...config, system: { ...config.system, adminPassword: undefined, jwtSecret: undefined } };
        res.json({ success: true, config: sanitized });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

app.get('/api/user/:userId', async (req, res) => {
    try {
        const user = await userManager.getUser(req.params.userId);
        if (!user) return res.status(404).json({ success: false, error: 'User not found' });
        const rank = eloSystem.getRank(user.elo || 1000);
        res.json({ success: true, user: { ...user, rank, rankProgress: eloSystem.getRankProgress(user.elo || 1000) } });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

app.get('/api/leaderboard', async (req, res) => {
    try {
        const limit = Math.min(parseInt(req.query.limit) || 100, 100);
        const type = req.query.type || 'elo';
        const leaderboard = await userManager.getLeaderboard(type, limit);
        res.json({ success: true, leaderboard });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

// ============================================
// 🔑 واجهات الإدارة
// ============================================
const authenticateAdmin = (req, res, next) => {
    const token = req.headers['authorization']?.split(' ')[1];
    if (!token) return res.status(401).json({ success: false, error: 'No token provided' });
    try {
        const config = loadServerConfig();
        const decoded = jwt.verify(token, config.system.jwtSecret || process.env.JWT_SECRET || 'default-secret');
        if (!decoded.isAdmin) return res.status(403).json({ success: false, error: 'Not authorized' });
        req.user = decoded;
        next();
    } catch (error) {
        return res.status(403).json({ success: false, error: 'Invalid token' });
    }
};

app.post('/api/admin/login', async (req, res) => {
    try {
        const { password } = req.body;
        const config = await loadServerConfig();
        if (antiCheat.verifyAdminPassword(password)) {
            const token = jwt.sign(
                { isAdmin: true, timestamp: Date.now() },
                config.system.jwtSecret || process.env.JWT_SECRET || 'default-secret',
                { expiresIn: '1h' }
            );
            monitoring.recordAdminAction('login');
            res.json({ success: true, token, expiresIn: 3600 });
        } else {
            monitoring.recordAdminAction('login_failed');
            res.status(401).json({ success: false, error: 'Invalid admin password' });
        }
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

app.get('/api/admin/stats', authenticateAdmin, async (req, res) => {
    try {
        const stats = {
            monitoring: monitoring.getStats(),
            locks: lockSystem.getStats(),
            antiCheat: antiCheat.getStats(),
            queue: queueProcessor.getStats(),
            database: db.getHealth(),
            cache: cache.getStats(),
            rooms: {
                total: roomManager.rooms.size,
                active: Array.from(roomManager.rooms.values()).filter(r => r.status === 'active').length,
                waiting: Array.from(roomManager.rooms.values()).filter(r => r.status === 'waiting').length,
                players: userManager.getOnlineCount()
            },
            games: {
                active: roomManager.activeGames.size,
                total: monitoring.metrics.games.total,
                completed: monitoring.metrics.games.completed
            },
            server: {
                uptime: monitoring.formatUptime(Math.floor((Date.now() - monitoring.startTime) / 1000)),
                memory: process.memoryUsage().heapUsed / 1024 / 1024
            }
        };
        res.json({ success: true, stats });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

app.post('/api/admin/ban', authenticateAdmin, async (req, res) => {
    try {
        const { userId, reason } = req.body;
        if (!userId) return res.status(400).json({ success: false, error: 'User ID required' });
        await antiCheat.banUser(userId, reason || 'Banned by admin');
        const player = userManager.getOnline(userId);
        if (player) {
            const socket = io.sockets.sockets.get(player.socketId);
            if (socket) { socket.emit('banned', { reason: reason || 'تم حظرك' }); socket.disconnect(); }
        }
        monitoring.recordAdminAction('ban_user', userId);
        res.json({ success: true, message: `User ${userId} banned` });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

app.post('/api/admin/unban', authenticateAdmin, async (req, res) => {
    try {
        const { userId } = req.body;
        if (!userId) return res.status(400).json({ success: false, error: 'User ID required' });
        const result = await antiCheat.unbanUser(userId);
        if (result) {
            monitoring.recordAdminAction('unban_user', userId);
            res.json({ success: true, message: `User ${userId} unbanned` });
        } else {
            res.status(404).json({ success: false, error: 'User not found or not banned' });
        }
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

app.post('/api/admin/balance', authenticateAdmin, async (req, res) => {
    try {
        const { userId, amount } = req.body;
        if (!userId || amount === undefined) {
            return res.status(400).json({ success: false, error: 'User ID and amount required' });
        }
        const newBalance = await userManager.addBalance(userId, amount, 'Admin adjustment');
        if (newBalance === false) return res.status(500).json({ success: false, error: 'Failed to update balance' });
        monitoring.recordAdminAction('balance', userId);
        res.json({ success: true, newBalance });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

app.post('/api/admin/config', authenticateAdmin, async (req, res) => {
    try {
        const { config } = req.body;
        if (!config) return res.status(400).json({ success: false, error: 'Config required' });
        await saveServerConfig(config);
        monitoring.recordAdminAction('update_config');
        res.json({ success: true, message: 'Config updated successfully' });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

app.post('/api/admin/maintenance', authenticateAdmin, async (req, res) => {
    try {
        const { enabled } = req.body;
        const config = await loadServerConfig();
        config.system.maintenanceMode = enabled;
        await saveServerConfig(config);
        if (enabled) {
            io.emit('maintenance_mode', { enabled: true, message: '🔧 الخادم في وضع الصيانة' });
        }
        monitoring.recordAdminAction('toggle_maintenance');
        res.json({ success: true, message: `Maintenance mode ${enabled ? 'enabled' : 'disabled'}` });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

// ============================================
// 🔌 WebSocket Server
// ============================================
const io = socketIo(server, {
    cors: {
        origin: process.env.CORS_ORIGIN?.split(',') || '*',
        methods: ['GET', 'POST'],
        credentials: true
    },
    transports: ['websocket', 'polling'],
    pingTimeout: 60000,
    pingInterval: 25000,
    maxHttpBufferSize: 1e6,
    path: '/socket.io',
    connectTimeout: 30000
});

io.use(async (socket, next) => {
    try {
        const userId = socket.handshake.query.userId;
        if (!userId) return next(new Error('User ID required'));
        
        if (!antiCheat.checkRateLimit(userId, 'auth', socket.handshake.address)) {
            return next(new Error('Too many authentication attempts'));
        }
        
        if (await antiCheat.isUserBanned(userId)) {
            return next(new Error('User is banned'));
        }
        
        const config = await loadServerConfig();
        if (config.system.maintenanceMode) {
            return next(new Error('Server is in maintenance mode'));
        }
        
        socket.userId = userId;
        socket.userData = await userManager.getUser(userId);
        socket.connectedAt = Date.now();
        next();
    } catch (error) {
        logger.error('Socket auth error:', { error: error.message });
        next(new Error('Authentication failed'));
    }
});

io.on('connection', (socket) => {
    const userId = socket.userId;
    logger.info(`Socket connected: ${socket.id} for user ${userId}`);
    monitoring.recordConnection('connect');
    userManager.setOnline(userId, socket.id);
    
    socket.emit('connected', {
        userId,
        socketId: socket.id,
        timestamp: Date.now(),
        serverTime: Date.now(),
        version: '12.0.0'
    });
    
    roomManager.broadcastRoomsList();
    roomManager.broadcastLobbyStats();
    
    socket.on('ping', (data) => {
        const latency = Date.now() - data.time;
        socket.emit('pong', { time: Date.now(), latency, serverTime: Date.now() });
        const player = userManager.getOnline(userId);
        if (player) player.lastPing = Date.now();
    });
    
    socket.on('join_room', async (data) => {
        const startTime = Date.now();
        try {
            if (!data.roomId) { socket.emit('error', { message: 'Room ID required' }); return; }
            if (!antiCheat.checkRateLimit(userId, 'join', socket.handshake.address)) {
                socket.emit('error', { message: 'Too many join attempts' }); return;
            }
            const room = await roomManager.joinRoom(socket, userId, data.roomId);
            monitoring.recordRequest(!!room, Date.now() - startTime);
        } catch (error) {
            logger.error('Join room error:', { error: error.message, userId });
            socket.emit('error', { message: error.message });
            monitoring.recordRequest(false, Date.now() - startTime);
            monitoring.recordError('join_room_error', error.message, 'socket');
        }
    });
    
    socket.on('leave_room', async (data) => {
        const startTime = Date.now();
        try {
            if (!data.roomId) { socket.emit('error', { message: 'Room ID required' }); return; }
            await roomManager.leaveRoom(socket, userId, data.roomId);
            monitoring.recordRequest(true, Date.now() - startTime);
        } catch (error) {
            logger.error('Leave room error:', { error: error.message, userId });
            socket.emit('error', { message: error.message });
            monitoring.recordRequest(false, Date.now() - startTime);
            monitoring.recordError('leave_room_error', error.message, 'socket');
        }
    });
    
    socket.on('player_move', (data) => {
        const player = userManager.getOnline(userId);
        if (!player?.currentRoomId) return;
        if (!antiCheat.checkRateLimit(userId, 'move', socket.handshake.address)) return;
        
        const game = roomManager.activeGames.get(player.currentRoomId);
        if (!game) return;
        const tank = game.tanks.get(userId);
        if (!tank || tank.health <= 0) return;
        
        tank.position = { ...data.position };
        tank.rotation = data.rotation || tank.rotation;
        socket.to(player.currentRoomId).emit('player_moved', {
            userId,
            position: data.position,
            rotation: data.rotation,
            timestamp: Date.now()
        });
    });
    
    socket.on('player_shoot', (data) => {
        const player = userManager.getOnline(userId);
        if (!player?.currentRoomId) return;
        if (!antiCheat.checkRateLimit(userId, 'shoot', socket.handshake.address)) return;
        
        const game = roomManager.activeGames.get(player.currentRoomId);
        if (!game) return;
        const tank = game.tanks.get(userId);
        if (!tank || tank.health <= 0) return;
        
        const bullet = {
            id: `bullet_${Date.now()}_${game.bulletId++}`,
            ownerId: userId,
            position: { ...data.position },
            velocity: {
                x: data.direction.x * (game.config?.game?.bulletSpeed || 2.8),
                z: data.direction.z * (game.config?.game?.bulletSpeed || 2.8)
            },
            damage: game.config?.game?.bulletDamage || 25,
            life: 200,
            timestamp: Date.now()
        };
        game.bullets.push(bullet);
        io.to(player.currentRoomId).emit('bullet_fired', {
            bulletId: bullet.id,
            ownerId: userId,
            position: data.position,
            direction: data.direction,
            timestamp: Date.now()
        });
    });
    
    socket.on('get_leaderboard', async () => {
        try {
            const leaderboard = await userManager.getLeaderboard('elo', 100);
            socket.emit('leaderboard_update', { leaderboard });
        } catch (error) {
            socket.emit('error', { message: 'Failed to get leaderboard' });
        }
    });
    
    socket.on('get_stats', async () => {
        try {
            const user = await userManager.getUser(userId);
            if (user) {
                const rank = eloSystem.getRank(user.elo || 1000);
                socket.emit('stats_update', { ...user, rank, rankProgress: eloSystem.getRankProgress(user.elo || 1000) });
            }
        } catch (error) {
            socket.emit('error', { message: 'Failed to get stats' });
        }
    });
    
    socket.on('disconnect', () => {
        logger.info(`Socket disconnected: ${socket.id} for user ${userId}`);
        monitoring.recordConnection('disconnect');
        userManager.setOffline(userId);
        roomManager.broadcastLobbyStats();
    });
});

// ============================================
// 🚀 بدء تشغيل الخادم
// ============================================
const PORT = process.env.PORT || 10000;

async function startServer() {
    try {
        await db.connect();
        await initializeDatabase();
        await loadServerConfig();
        
        server.listen(PORT, '0.0.0.0', () => {
            console.log(`
╔══════════════════════════════════════════════════════════════════════════════╗
║                                                                              ║
║     🎮 BATTLE TANKS ROYALE - v12.0.0 COMPLETE EDITION 🎮                   ║
║                                                                              ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  📡 Server: http://0.0.0.0:${PORT}
║  🗄️ Database: PostgreSQL ✅ Connected
║  ⚡ WebSocket: Ready
║  🎯 Mode: Battle Royale
║                                                                              ║
║  🛡️ Anti-Cheat: ${antiCheat.enabled ? '✅ ENABLED' : '❌ DISABLED'}
║  🔒 Lock System: ✅ ACTIVE
║  📊 Queue System: ✅ ACTIVE
║  📈 Monitoring: ✅ ACTIVE
║  💾 Cache: ✅ ACTIVE
║                                                                              ║
║  🏠 Rooms: ${roomManager.rooms.size} available
║  👥 Players: ${userManager.getOnlineCount()} online
║  🎮 Games: ${roomManager.activeGames.size} active
║                                                                              ║
║  📊 API Endpoints:
║     - GET  /health
║     - GET  /metrics
║     - GET  /api/config
║     - GET  /api/user/:userId
║     - GET  /api/leaderboard
║     - POST /api/admin/login
║     - GET  /api/admin/stats
║     - POST /api/admin/ban
║     - POST /api/admin/unban
║     - POST /api/admin/balance
║     - POST /api/admin/config
║     - POST /api/admin/maintenance
║                                                                              ║
║  🔄 Database Auto-Reconnect: ✅ ENABLED
║  ⚡ Circuit Breaker: ✅ ENABLED
║  💾 Cache: ✅ ACTIVE
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝
            `);
        });
        
    } catch (error) {
        logger.error('Failed to start server:', { error: error.message });
        console.error('❌ Failed to start server:', error.message);
        console.log('🔄 Retrying in 10 seconds...');
        setTimeout(startServer, 10000);
    }
}

async function initializeDatabase() {
    try {
        await db.query(`
            CREATE TABLE IF NOT EXISTS users (
                id VARCHAR(64) PRIMARY KEY,
                telegram_id VARCHAR(64) UNIQUE,
                username VARCHAR(100),
                balance INTEGER DEFAULT 100,
                elo INTEGER DEFAULT 1000,
                kills INTEGER DEFAULT 0,
                wins INTEGER DEFAULT 0,
                games_played INTEGER DEFAULT 0,
                total_rewards INTEGER DEFAULT 0,
                is_admin BOOLEAN DEFAULT FALSE,
                is_banned BOOLEAN DEFAULT FALSE,
                ban_reason TEXT,
                banned_until TIMESTAMP,
                banned_by VARCHAR(64),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_login TIMESTAMP,
                last_game TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE TABLE IF NOT EXISTS rooms (
                id VARCHAR(64) PRIMARY KEY,
                type VARCHAR(32),
                name VARCHAR(100),
                max_seats INTEGER DEFAULT 8,
                seat_price INTEGER DEFAULT 1,
                reward_multiplier FLOAT DEFAULT 1.0,
                status VARCHAR(32) DEFAULT 'waiting',
                players JSONB DEFAULT '[]',
                spectators JSONB DEFAULT '[]',
                game_round INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                stats JSONB DEFAULT '{}'
            );
            
            CREATE TABLE IF NOT EXISTS matches (
                id VARCHAR(64) PRIMARY KEY,
                room_id VARCHAR(64),
                winner_id VARCHAR(64),
                players JSONB,
                kill_feed JSONB,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                duration INTEGER,
                total_players INTEGER,
                game_round INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE TABLE IF NOT EXISTS server_config (
                key VARCHAR(64) PRIMARY KEY,
                value JSONB,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE TABLE IF NOT EXISTS visual_settings (
                id VARCHAR(64) PRIMARY KEY,
                event_key VARCHAR(64) UNIQUE,
                image_url TEXT,
                alt_text VARCHAR(255),
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE TABLE IF NOT EXISTS transactions (
                id VARCHAR(64) PRIMARY KEY,
                user_id VARCHAR(64),
                type VARCHAR(32),
                amount INTEGER,
                balance_before INTEGER,
                balance_after INTEGER,
                description TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE INDEX IF NOT EXISTS idx_users_elo ON users(elo DESC);
            CREATE INDEX IF NOT EXISTS idx_users_wins ON users(wins DESC);
            CREATE INDEX IF NOT EXISTS idx_users_kills ON users(kills DESC);
            CREATE INDEX IF NOT EXISTS idx_matches_room ON matches(room_id);
            CREATE INDEX IF NOT EXISTS idx_matches_winner ON matches(winner_id);
            CREATE INDEX IF NOT EXISTS idx_transactions_user ON transactions(user_id);
            CREATE INDEX IF NOT EXISTS idx_transactions_created ON transactions(created_at DESC);
        `);
        logger.info('✅ Database schema initialized');
    } catch (error) {
        logger.error('Error initializing database:', { error: error.message });
        throw error;
    }
}

async function shutdown() {
    logger.info('Shutting down gracefully...');
    console.log('🛑 Shutting down gracefully...');
    server.close(() => logger.info('HTTP server closed'));
    io.close(() => logger.info('WebSocket server closed'));
    await db.shutdown();
    await cache.shutdown();
    await lockSystem.shutdown();
    monitoring.stop();
    process.exit(0);
}

process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

startServer();

module.exports = { app, server, io, db, monitoring, lockSystem, antiCheat, cache, queueProcessor, roomManager, userManager, eloSystem, loadServerConfig, saveServerConfig };
