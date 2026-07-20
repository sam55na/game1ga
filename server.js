// ============================================
// 🚀 BATTLE TANKS ROYALE - الخادم النهائي المتكامل
// ============================================
// Version: 11.0.0 - ULTIMATE EDITION
// Architecture: Enterprise Grade + Microservices Ready
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
const { StatusCodes } = require('http-status-codes');
const jwt = require('jsonwebtoken');
const rateLimit = require('express-rate-limit');
const compression = require('compression');
const { v4: uuidv4 } = require('uuid');
const { createClient } = require('redis');
const cluster = require('cluster');
const os = require('os');

// ============================================
// 📊 CLUSTER MODE (Multi-core support)
// ============================================
const isProduction = process.env.NODE_ENV === 'production';
const numCPUs = isProduction ? os.cpus().length : 1;

if (cluster.isMaster && isProduction) {
    console.log(`🔄 Master ${process.pid} is running`);
    
    // Fork workers
    for (let i = 0; i < numCPUs; i++) {
        cluster.fork();
    }
    
    cluster.on('exit', (worker, code, signal) => {
        console.log(`❌ Worker ${worker.process.pid} died. Restarting...`);
        cluster.fork();
    });
    
    // استمرار الماستر
    return;
}

// ============================================
// 📝 نظام التسجيل المركزي المتطور
// ============================================
class AdvancedLogger {
    constructor() {
        this.loggers = {};
        this.currentLevel = process.env.LOG_LEVEL || 'info';
        
        // تكوين الـ Winston مع دعم ELK
        const format = winston.format.combine(
            winston.format.timestamp({ format: 'ISO-8601' }),
            winston.format.errors({ stack: true }),
            winston.format.metadata(),
            winston.format.json()
        );
        
        // Logger رئيسي
        this.mainLogger = winston.createLogger({
            level: this.currentLevel,
            format,
            defaultMeta: { 
                service: 'battle-tanks-royale', 
                version: '11.0.0',
                worker: cluster.worker ? cluster.worker.id : 'main',
                pid: process.pid
            },
            transports: [
                new winston.transports.Console({
                    format: winston.format.combine(
                        winston.format.colorize(),
                        winston.format.printf(({ timestamp, level, message, metadata }) => {
                            return `${timestamp} [${level}] ${message} ${JSON.stringify(metadata || {})}`;
                        })
                    )
                }),
                new winston.transports.File({
                    filename: 'logs/error.log',
                    level: 'error',
                    maxsize: 10485760, // 10MB
                    maxFiles: 10,
                    tailable: true
                }),
                new winston.transports.File({
                    filename: 'logs/combined.log',
                    maxsize: 10485760,
                    maxFiles: 10,
                    tailable: true
                }),
                new winston.transports.File({
                    filename: 'logs/audit.log',
                    level: 'info',
                    maxsize: 10485760,
                    maxFiles: 10,
                    tailable: true
                }),
                new winston.transports.File({
                    filename: 'logs/performance.log',
                    level: 'performance',
                    maxsize: 10485760,
                    maxFiles: 5,
                    tailable: true
                })
            ]
        });
        
        // إضافة سياق
        this.mainLogger.withContext = (context) => {
            return this.mainLogger.child({ context });
        };
        
        // مستويات مخصصة
        this.mainLogger.performance = (message, metadata = {}) => {
            this.mainLogger.log('performance', message, { metadata });
        };
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
    
    performance(message, metadata = {}) {
        this.mainLogger.performance(message, { metadata });
    }
    
    audit(action, userId, details = {}) {
        this.mainLogger.info(`AUDIT: ${action}`, {
            metadata: { 
                userId, 
                action, 
                details, 
                timestamp: new Date().toISOString(),
                worker: cluster.worker ? cluster.worker.id : 'main'
            }
        });
    }
}

const logger = new AdvancedLogger();

// ============================================
// 🔄 Circuit Breaker المتقدم
// ============================================
class AdvancedCircuitBreaker {
    constructor(options = {}) {
        this.failureThreshold = options.failureThreshold || 5;
        this.timeout = options.timeout || 60000;
        this.halfOpenTimeout = options.halfOpenTimeout || 30000;
        this.failureCount = 0;
        this.state = 'CLOSED';
        this.lastFailureTime = null;
        this.timer = null;
        this.metrics = {
            totalRequests: 0,
            successfulRequests: 0,
            failedRequests: 0,
            rejectedRequests: 0
        };
        this.eventEmitter = new EventEmitter();
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
                logger.info('✅ Circuit breaker: CLOSED (recovered)');
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
                this.eventEmitter.emit('open', { failureCount: this.failureCount, lastFailure: this.lastFailureTime });
            }
            
            if (fallback) return fallback();
            throw error;
        }
    }
    
    reset() {
        this.failureCount = 0;
        this.state = 'CLOSED';
        this.lastFailureTime = null;
        if (this.timer) {
            clearTimeout(this.timer);
            this.timer = null;
        }
        this.eventEmitter.emit('reset');
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
            connections: { total: 0, active: 0, peak: 0, historical: [], byIP: new Map() },
            requests: {
                total: 0,
                success: 0,
                error: 0,
                rate: 0,
                byEndpoint: {},
                byStatus: {},
                byMethod: {},
                responseTime: {
                    avg: 0,
                    max: 0,
                    min: Infinity,
                    p50: 0,
                    p90: 0,
                    p95: 0,
                    p99: 0,
                    histogram: []
                }
            },
            games: {
                total: 0,
                active: 0,
                completed: 0,
                averageDuration: 0,
                maxPlayers: 0,
                byType: {}
            },
            errors: {
                total: 0,
                byType: {},
                byService: {},
                byUser: {},
                recent: [],
                rate: 0
            },
            performance: {
                avgResponseTime: 0,
                maxResponseTime: 0,
                p95ResponseTime: 0,
                p99ResponseTime: 0,
                responseTimes: [],
                memory: { heapUsed: 0, heapTotal: 0, external: 0, rss: 0 },
                cpu: { user: 0, system: 0, usage: 0 }
            },
            database: {
                connected: false,
                reconnectAttempts: 0,
                lastError: null,
                queryCount: 0,
                avgQueryTime: 0,
                slowQueries: 0,
                poolSize: 0,
                idleCount: 0,
                waitingCount: 0
            },
            admin: { logins: 0, actions: 0, failedLogins: 0, lastLogin: null },
            system: {
                cpu: 0,
                memory: 0,
                uptime: 0,
                loadAverage: [0, 0, 0],
                totalMemory: 0,
                freeMemory: 0
            },
            network: {
                bytesIn: 0,
                bytesOut: 0,
                packetsIn: 0,
                packetsOut: 0
            },
            business: {
                totalUsers: 0,
                activeUsers: 0,
                totalBalance: 0,
                averageBalance: 0,
                totalKills: 0,
                totalWins: 0
            }
        };
        
        this.startTime = Date.now();
        this.requestTimestamps = [];
        this.errorLogs = [];
        this.maxErrorLogs = 1000;
        this.responseTimeBuffer = [];
        this.maxBufferSize = 10000;
        this.metricsInterval = null;
        this.histogramBuckets = [0, 10, 25, 50, 100, 200, 500, 1000, 2000, 5000];
        
        this.startMetricsCollection();
        this.startPerformanceMonitoring();
    }
    
    startMetricsCollection() {
        this.metricsInterval = setInterval(() => {
            this.collectSystemMetrics();
            this.calculatePercentiles();
            this.updateErrorRate();
            this.cleanupHistoricalData();
        }, 30000);
    }
    
    startPerformanceMonitoring() {
        // مراقبة الأداء كل 10 ثواني
        setInterval(() => {
            const mem = process.memoryUsage();
            this.metrics.performance.memory = {
                heapUsed: mem.heapUsed / 1024 / 1024,
                heapTotal: mem.heapTotal / 1024 / 1024,
                external: mem.external / 1024 / 1024,
                rss: mem.rss / 1024 / 1024
            };
            
            const cpu = process.cpuUsage();
            this.metrics.performance.cpu = {
                user: cpu.user / 1000000,
                system: cpu.system / 1000000,
                usage: (cpu.user + cpu.system) / 1000000 / 10
            };
            
            // Load average (Unix only)
            try {
                const load = os.loadavg();
                this.metrics.system.loadAverage = load;
            } catch (e) {}
            
            this.metrics.system.totalMemory = os.totalmem() / 1024 / 1024;
            this.metrics.system.freeMemory = os.freemem() / 1024 / 1024;
        }, 10000);
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
        
        this.metrics.performance.p95ResponseTime = sorted[Math.floor(len * 0.95)] || 0;
        this.metrics.performance.p99ResponseTime = sorted[Math.floor(len * 0.99)] || 0;
        this.metrics.performance.p50ResponseTime = sorted[Math.floor(len * 0.5)] || 0;
        this.metrics.performance.p90ResponseTime = sorted[Math.floor(len * 0.9)] || 0;
        
        // تحديث الهيستوغرام
        this.updateHistogram(sorted);
        
        if (this.responseTimeBuffer.length > this.maxBufferSize) {
            this.responseTimeBuffer = this.responseTimeBuffer.slice(-this.maxBufferSize);
        }
    }
    
    updateHistogram(sorted) {
        const histogram = {};
        for (const bucket of this.histogramBuckets) {
            histogram[bucket] = 0;
        }
        
        for (const val of sorted) {
            let found = false;
            for (const bucket of this.histogramBuckets) {
                if (val <= bucket) {
                    histogram[bucket]++;
                    found = true;
                    break;
                }
            }
            if (!found) {
                histogram[this.histogramBuckets[this.histogramBuckets.length - 1]]++;
            }
        }
        
        // تحويل إلى نسب مئوية
        const total = sorted.length;
        for (const bucket of this.histogramBuckets) {
            histogram[bucket] = (histogram[bucket] / total) * 100;
        }
        
        this.metrics.performance.responseTime.histogram = histogram;
    }
    
    updateErrorRate() {
        const now = Date.now();
        const oneMinuteAgo = now - 60000;
        const recentErrors = this.errorLogs.filter(e => e.timestamp > oneMinuteAgo);
        this.metrics.errors.rate = recentErrors.length / 60;
    }
    
    cleanupHistoricalData() {
        const maxAge = 3600000; // 1 hour
        const now = Date.now();
        
        // تنظيف سجلات الأخطاء القديمة
        this.errorLogs = this.errorLogs.filter(e => now - e.timestamp < maxAge);
        
        // تنظيف سجلات الاتصالات القديمة
        this.metrics.connections.historical = this.metrics.connections.historical.filter(
            e => now - e.timestamp < maxAge
        );
        if (this.metrics.connections.historical.length > 1000) {
            this.metrics.connections.historical = this.metrics.connections.historical.slice(-1000);
        }
    }
    
    recordConnection(type, ip = null) {
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
            
            if (ip) {
                this.metrics.connections.byIP.set(ip, (this.metrics.connections.byIP.get(ip) || 0) + 1);
            }
        } else if (type === 'disconnect') {
            this.metrics.connections.active = Math.max(0, this.metrics.connections.active - 1);
        }
    }
    
    recordRequest(success, duration, endpoint = 'unknown', method = 'GET', status = 200) {
        this.metrics.requests.total++;
        if (success) {
            this.metrics.requests.success++;
        } else {
            this.metrics.requests.error++;
        }
        
        this.metrics.requests.byEndpoint[endpoint] = (this.metrics.requests.byEndpoint[endpoint] || 0) + 1;
        this.metrics.requests.byStatus[status] = (this.metrics.requests.byStatus[status] || 0) + 1;
        this.metrics.requests.byMethod[method] = (this.metrics.requests.byMethod[method] || 0) + 1;
        
        this.requestTimestamps.push(Date.now());
        if (this.requestTimestamps.length > 1000) {
            this.requestTimestamps.shift();
        }
        
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
            
            // Performance log for slow requests
            if (duration > 1000) {
                logger.performance('Slow request detected', {
                    duration,
                    endpoint,
                    method,
                    status
                });
            }
        }
    }
    
    recordError(errorType, errorDetails = null, service = 'unknown', userId = null) {
        this.metrics.errors.total++;
        this.metrics.errors.byType[errorType] = (this.metrics.errors.byType[errorType] || 0) + 1;
        this.metrics.errors.byService[service] = (this.metrics.errors.byService[service] || 0) + 1;
        
        if (userId) {
            this.metrics.errors.byUser[userId] = (this.metrics.errors.byUser[userId] || 0) + 1;
        }
        
        const errorLog = {
            type: errorType,
            timestamp: Date.now(),
            details: errorDetails,
            service,
            userId
        };
        this.errorLogs.push(errorLog);
        if (this.errorLogs.length > this.maxErrorLogs) {
            this.errorLogs.shift();
        }
        
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
        this.metrics.database.avgQueryTime = 
            (this.metrics.database.avgQueryTime * 0.9) + (duration * 0.1);
        
        if (duration > 1000) {
            this.metrics.database.slowQueries++;
        }
    }
    
    recordAdminAction(type, userId = null) {
        if (type === 'login') {
            this.metrics.admin.logins++;
            this.metrics.admin.lastLogin = Date.now();
        } else if (type === 'login_failed') {
            this.metrics.admin.failedLogins++;
        } else {
            this.metrics.admin.actions++;
        }
        
        if (userId) {
            logger.audit(type, userId);
        }
    }
    
    recordGameStarted(players, type = 'battle_royale') {
        this.metrics.games.total++;
        this.metrics.games.active++;
        if (players > this.metrics.games.maxPlayers) {
            this.metrics.games.maxPlayers = players;
        }
        this.metrics.games.byType[type] = (this.metrics.games.byType[type] || 0) + 1;
    }
    
    recordGameEnded(duration, type = 'battle_royale') {
        this.metrics.games.active = Math.max(0, this.metrics.games.active - 1);
        this.metrics.games.completed++;
        this.metrics.games.averageDuration = 
            (this.metrics.games.averageDuration * 0.9) + (duration * 0.1);
        this.metrics.games.byType[type] = (this.metrics.games.byType[type] || 0);
    }
    
    recordBusinessMetrics(data) {
        this.metrics.business = { ...this.metrics.business, ...data };
    }
    
    getStats() {
        const uptime = Math.floor((Date.now() - this.startTime) / 1000);
        return {
            ...this.metrics,
            uptime,
            uptimeFormatted: this.formatUptime(uptime),
            errorLogs: this.errorLogs.slice(-50),
            timestamp: new Date().toISOString(),
            worker: cluster.worker ? cluster.worker.id : 'main',
            pid: process.pid
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
            database: {
                connected: this.metrics.database.connected,
                poolSize: this.metrics.database.poolSize,
                queryCount: this.metrics.database.queryCount
            },
            errors: {
                total: this.metrics.errors.total,
                rate: this.metrics.errors.rate,
                recent: this.errorLogs.slice(-5)
            },
            system: {
                cpu: this.metrics.system.cpu,
                memory: this.metrics.system.memory,
                uptime: this.metrics.system.uptime
            },
            games: {
                active: this.metrics.games.active,
                total: this.metrics.games.total
            },
            timestamp: new Date().toISOString()
        };
    }
    
    getPrometheusMetrics() {
        return {
            connections_active: this.metrics.connections.active,
            connections_total: this.metrics.connections.total,
            requests_total: this.metrics.requests.total,
            requests_rate: this.metrics.requests.rate,
            errors_total: this.metrics.errors.total,
            errors_rate: this.metrics.errors.rate,
            games_active: this.metrics.games.active,
            games_total: this.metrics.games.total,
            games_completed: this.metrics.games.completed,
            response_time_avg: this.metrics.requests.responseTime.avg,
            response_time_p95: this.metrics.performance.p95ResponseTime,
            response_time_p99: this.metrics.performance.p99ResponseTime,
            database_connected: this.metrics.database.connected ? 1 : 0,
            database_query_count: this.metrics.database.queryCount,
            system_memory_mb: this.metrics.system.memory,
            system_cpu: this.metrics.system.cpu,
            uptime_seconds: Math.floor((Date.now() - this.startTime) / 1000),
            admin_logins: this.metrics.admin.logins,
            admin_actions: this.metrics.admin.actions,
            total_users: this.metrics.business.totalUsers,
            active_users: this.metrics.business.activeUsers,
            total_balance: this.metrics.business.totalBalance,
            total_kills: this.metrics.business.totalKills,
            total_wins: this.metrics.business.totalWins
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
        this.circuitBreaker = new AdvancedCircuitBreaker({
            failureThreshold: 3,
            timeout: 30000,
            halfOpenTimeout: 10000
        });
        this.poolConfig = {
            max: process.env.DB_POOL_MAX || 50,
            idleTimeoutMillis: 30000,
            connectionTimeoutMillis: 15000,
            maxUses: 7500,
            allowExitOnIdle: false,
            statement_timeout: 10000,
            query_timeout: 10000
        };
        this.preparedStatements = new Map();
        this.migrationLock = false;
        this.schemaVersion = 0;
        
        if (!this.connectionString) {
            logger.error('DATABASE_URL is not set');
            this.connectionString = process.env.DATABASE_URL || 
                'postgresql://neondb_owner:npg_MSOwr97htVJu@ep-patient-dawn-awed2uh0-pooler.c-12.us-east-1.aws.neon.tech/neondb?sslmode=require';
        }
        
        logger.info('Advanced Database Manager initialized');
    }
    
    async connect() {
        if (this.isReconnecting) {
            logger.debug('Reconnection already in progress');
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
                logger.error('Unexpected database pool error:', { error: err.message });
                this.isConnected = false;
                monitoring.recordDatabaseStatus(false, err.message);
                this.handleReconnect();
            });
            
            this.pool.on('connect', () => {
                logger.info('New database client connected');
            });
            
            this.pool.on('remove', () => {
                logger.debug('Database client removed from pool');
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
            this.metrics.database.poolSize = this.pool.totalCount;
            this.metrics.database.idleCount = this.pool.idleCount;
            this.metrics.database.waitingCount = this.pool.waitingCount;
            monitoring.recordDatabaseStatus(true);
            monitoring.recordDatabaseQuery(duration);
            
            // تحديث السكيمات
            await this.initializeSchema();
            
            if (this.pingInterval) clearInterval(this.pingInterval);
            this.pingInterval = setInterval(() => this.healthCheck(), 15000);
            
            logger.info('✅ Database connected successfully', { duration, poolSize: this.pool.totalCount });
            return this.pool;
            
        } catch (error) {
            logger.error('Database connection failed:', { error: error.message, stack: error.stack });
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
            // إنشاء الجداول إذا لم تكن موجودة
            const schema = `
                CREATE TABLE IF NOT EXISTS users (
                    id VARCHAR(64) PRIMARY KEY,
                    telegram_id VARCHAR(64) UNIQUE,
                    username VARCHAR(100),
                    balance INTEGER DEFAULT 0,
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
                    game_round INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
            
            // تحديث مقاييس التجمع
            if (this.pool) {
                this.metrics.database.poolSize = this.pool.totalCount;
                this.metrics.database.idleCount = this.pool.idleCount;
                this.metrics.database.waitingCount = this.pool.waitingCount;
            }
            
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
        
        logger.warn(`Reconnecting attempt ${this.reconnectAttempts} in ${Math.round(delay/1000)}s...`);
        
        return new Promise((resolve) => {
            if (this.reconnectTimer) {
                clearTimeout(this.reconnectTimer);
            }
            
            this.reconnectTimer = setTimeout(async () => {
                logger.info(`Attempting reconnection ${this.reconnectAttempts}...`);
                try {
                    await this.connect();
                    resolve(this.pool);
                } catch (error) {
                    logger.error(`Reconnection ${this.reconnectAttempts} failed:`, { error: error.message });
                    this.isReconnecting = false;
                    this.handleReconnect().then(resolve);
                }
            }, delay);
        });
    }
    
    async query(text, params) {
        if (!this.isConnected || !this.pool) {
            logger.warn('Waiting for database connection...');
            await this.connect();
        }
        
        const startTime = Date.now();
        let result = null;
        
        try {
            result = await this.circuitBreaker.execute(async () => {
                return await this.pool.query(text, params);
            });
            
            const duration = Date.now() - startTime;
            monitoring.recordDatabaseQuery(duration);
            
            if (duration > 1000) {
                logger.warn('Slow query detected:', { 
                    query: text.substring(0, 200), 
                    duration,
                    params: params ? params.length : 0
                });
            }
            
            return result;
            
        } catch (error) {
            const duration = Date.now() - startTime;
            logger.error('Database query error:', { 
                error: error.message,
                code: error.code,
                query: text.substring(0, 200),
                duration
            });
            
            // محاولة استعادة الاتصال لأخطاء معينة
            if (error.code === 'ECONNRESET' || 
                error.code === '57P01' || 
                error.code === '08003' ||
                error.code === '08006' ||
                error.message.includes('connection') ||
                error.message.includes('timeout')) {
                
                logger.warn('Connection lost, attempting to reconnect...');
                this.isConnected = false;
                monitoring.recordDatabaseStatus(false, error.message);
                
                try {
                    await this.connect();
                    logger.info('Reconnected, retrying query...');
                    const retryResult = await this.pool.query(text, params);
                    monitoring.recordDatabaseQuery(Date.now() - startTime);
                    return retryResult;
                } catch (reconnectError) {
                    logger.error('Reconnection failed for query:', { error: reconnectError.message });
                    throw new Error(`Database connection lost: ${reconnectError.message}`);
                }
            }
            
            monitoring.recordError('query_error', error.message, 'database');
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
            logger.error('Transaction error:', { error: error.message });
            monitoring.recordError('transaction_error', error.message, 'database');
            throw error;
        } finally {
            client.release();
        }
    }
    
    async prepareStatement(name, text) {
        if (this.preparedStatements.has(name)) {
            return this.preparedStatements.get(name);
        }
        
        try {
            const client = await this.pool.connect();
            await client.query(`PREPARE ${name} AS ${text}`);
            client.release();
            this.preparedStatements.set(name, { text, prepared: true });
            logger.debug(`Statement prepared: ${name}`);
            return { name, text };
        } catch (error) {
            logger.error(`Failed to prepare statement ${name}:`, { error: error.message });
            throw error;
        }
    }
    
    async executePrepared(name, params) {
        try {
            const result = await this.query(`EXECUTE ${name}(${params.map((_, i) => `$${i+1}`).join(', ')})`, params);
            return result;
        } catch (error) {
            logger.error(`Failed to execute prepared statement ${name}:`, { error: error.message });
            // محاولة إعادة تحضير البيان
            this.preparedStatements.delete(name);
            throw error;
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
            },
            preparedStatements: this.preparedStatements.size
        };
    }
    
    async shutdown() {
        if (this.pingInterval) {
            clearInterval(this.pingInterval);
            this.pingInterval = null;
        }
        if (this.reconnectTimer) {
            clearTimeout(this.reconnectTimer);
            this.reconnectTimer = null;
        }
        if (this.pool) {
            await this.pool.end();
            logger.info('Database pool closed');
        }
    }
}

const db = new AdvancedDatabaseManager();

// ============================================
// 🔒 نظام القفل الموزع المتقدم
// ============================================
class AdvancedDistributedLock {
    constructor() {
        this.locks = new Map();
        this.waitingQueues = new Map();
        this.lockTimeouts = new Map();
        this.maxLockTime = 30000;
        this.cleanupInterval = setInterval(() => this.cleanup(), 60000);
        this.redisClient = null;
        this.useRedis = !!process.env.REDIS_URL;
        this.metrics = {
            totalLocks: 0,
            activeLocks: 0,
            totalWaits: 0,
            avgWaitTime: 0,
            lockTimeouts: 0
        };
        
        if (this.useRedis) {
            this.initRedis();
        }
        
        logger.info('Advanced Distributed Lock initialized', { useRedis: this.useRedis });
    }
    
    async initRedis() {
        try {
            this.redisClient = createClient({
                url: process.env.REDIS_URL,
                socket: {
                    reconnectStrategy: (retries) => Math.min(retries * 100, 3000)
                }
            });
            
            this.redisClient.on('error', (err) => {
                logger.error('Redis error:', { error: err.message });
                this.useRedis = false;
            });
            
            await this.redisClient.connect();
            logger.info('✅ Redis connected for distributed locking');
        } catch (error) {
            logger.error('Failed to connect to Redis:', { error: error.message });
            this.useRedis = false;
        }
    }
    
    async acquireLock(resourceId, userId, timeout = 10000) {
        const lockKey = `lock:${resourceId}`;
        const startTime = Date.now();
        this.metrics.totalLocks++;
        
        logger.debug(`Attempting to acquire lock for ${resourceId} (user: ${userId})`);
        
        try {
            if (this.useRedis && this.redisClient) {
                return await this.acquireRedisLock(lockKey, userId, timeout);
            }
            return await this.acquireMemoryLock(lockKey, userId, timeout);
        } catch (error) {
            this.metrics.lockTimeouts++;
            throw error;
        }
    }
    
    async acquireRedisLock(lockKey, userId, timeout) {
        const lockValue = `${userId}:${Date.now()}`;
        const acquired = await this.redisClient.set(lockKey, lockValue, {
            NX: true,
            PX: this.maxLockTime
        });
        
        if (acquired) {
            this.metrics.activeLocks++;
            logger.debug(`Redis lock acquired: ${lockKey}`);
            return true;
        }
        
        // انتظار القفل مع المهلة
        return new Promise((resolve, reject) => {
            const startTime = Date.now();
            const checkInterval = setInterval(async () => {
                if (Date.now() - startTime > timeout) {
                    clearInterval(checkInterval);
                    this.metrics.totalWaits++;
                    reject(new Error('Lock acquisition timeout'));
                    return;
                }
                
                const exists = await this.redisClient.exists(lockKey);
                if (!exists) {
                    clearInterval(checkInterval);
                    const newLock = await this.redisClient.set(lockKey, lockValue, {
                        NX: true,
                        PX: this.maxLockTime
                    });
                    if (newLock) {
                        this.metrics.activeLocks++;
                        logger.debug(`Redis lock acquired: ${lockKey}`);
                        resolve(true);
                    } else {
                        reject(new Error('Failed to acquire lock'));
                    }
                }
            }, 100);
        });
    }
    
    acquireMemoryLock(lockKey, userId, timeout) {
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
                logger.debug(`Added ${userId} to waiting queue for ${lockKey}`);
                return;
            }
            
            this.grantMemoryLock(lockKey, userId, resolve);
        });
    }
    
    grantMemoryLock(lockKey, userId, resolve) {
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
        logger.debug(`Lock ${lockKey} granted to ${userId}`);
        resolve(true);
    }
    
    async releaseLock(resourceId, userId) {
        const lockKey = `lock:${resourceId}`;
        
        if (this.useRedis && this.redisClient) {
            const result = await this.redisClient.del(lockKey);
            if (result > 0) {
                this.metrics.activeLocks = Math.max(0, this.metrics.activeLocks - 1);
                logger.debug(`Redis lock released: ${lockKey}`);
                return true;
            }
            return false;
        }
        
        return this.releaseMemoryLock(lockKey);
    }
    
    releaseMemoryLock(lockKey) {
        if (!this.locks.has(lockKey)) {
            logger.warn(`Attempted to release non-existent lock: ${lockKey}`);
            return false;
        }
        
        this.locks.delete(lockKey);
        this.metrics.activeLocks = Math.max(0, this.metrics.activeLocks - 1);
        logger.debug(`Lock ${lockKey} released`);
        
        if (this.lockTimeouts.has(lockKey)) {
            clearTimeout(this.lockTimeouts.get(lockKey));
            this.lockTimeouts.delete(lockKey);
        }
        
        // منح القفل للعميل التالي
        if (this.waitingQueues.has(lockKey)) {
            const queue = this.waitingQueues.get(lockKey);
            if (queue.length > 0) {
                const next = queue.shift();
                clearTimeout(next.timeoutId);
                const waitTime = Date.now() - next.startWait;
                this.metrics.avgWaitTime = (this.metrics.avgWaitTime * 0.9) + (waitTime * 0.1);
                this.grantMemoryLock(lockKey, next.userId, next.resolve);
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
                this.releaseMemoryLock(key);
                this.metrics.lockTimeouts++;
            }
        }
    }
    
    forceUnlock(resourceId, userId) {
        const lockKey = `lock:${resourceId}`;
        
        if (this.useRedis && this.redisClient) {
            this.redisClient.del(lockKey);
            this.metrics.activeLocks = Math.max(0, this.metrics.activeLocks - 1);
        }
        
        return this.releaseMemoryLock(lockKey);
    }
    
    getStats() {
        return {
            activeLocks: this.metrics.activeLocks,
            waitingQueues: this.waitingQueues.size,
            totalWaiting: Array.from(this.waitingQueues.values()).reduce(
                (sum, q) => sum + q.length, 0
            ),
            useRedis: this.useRedis,
            ...this.metrics
        };
    }
    
    async shutdown() {
        if (this.cleanupInterval) {
            clearInterval(this.cleanupInterval);
        }
        if (this.redisClient) {
            await this.redisClient.quit();
        }
    }
}

const lockSystem = new AdvancedDistributedLock();

// ============================================
// 🛡️ نظام الحماية المتقدم
// ============================================
class AdvancedAntiCheat {
    constructor() {
        this.actionTracker = new Map();
        this.rateLimits = {
            move: { max: 60, window: 1000, blockTime: 5000, penalty: 1 },
            shoot: { max: 5, window: 3000, blockTime: 10000, penalty: 2 },
            join: { max: 5, window: 10000, blockTime: 30000, penalty: 1 },
            auth: { max: 5, window: 5000, blockTime: 60000, penalty: 3 },
            admin: { max: 10, window: 60000, blockTime: 300000, penalty: 2 },
            chat: { max: 20, window: 10000, blockTime: 60000, penalty: 1 },
            reconnect: { max: 3, window: 30000, blockTime: 120000, penalty: 2 }
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
        this.globalBlockList = new Set();
        this.penaltyMultiplier = 1;
        this.threatScores = new Map();
        this.suspiciousPatterns = [
            { pattern: /wallhack|aimbot/i, action: 'aimbot', severity: 5 },
            { pattern: /speedhack|teleport/i, action: 'speedhack', severity: 4 },
            { pattern: /spam|flood/i, action: 'spam', severity: 2 },
            { pattern: /no_damage|godmode/i, action: 'godmode', severity: 5 },
            { pattern: /infinite_ammo/i, action: 'infinite_ammo', severity: 3 },
            { pattern: /fly|noclip/i, action: 'fly', severity: 4 }
        ];
        
        // أنظمة الكشف
        this.detectors = {
            speedHack: new SpeedHackDetector(),
            aimbot: new AimbotDetector(),
            teleport: new TeleportDetector(),
            spam: new SpamDetector(),
            godmode: new GodModeDetector()
        };
        
        logger.info('Advanced AntiCheat initialized', { enabled: this.enabled });
    }
    
    checkRateLimit(userId, actionType, ip = null) {
        if (!this.enabled) return true;
        
        if (this.globalBlockList.has(userId) || (ip && this.globalBlockList.has(ip))) {
            return false;
        }
        
        const now = Date.now();
        const limit = this.rateLimits[actionType];
        if (!limit) return true;
        
        const key = `${userId}:${actionType}`;
        
        if (this.blockedUntil.has(key) && this.blockedUntil.get(key) > now) {
            logger.warn(`Rate limit blocked: ${key} until ${new Date(this.blockedUntil.get(key))}`);
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
            this.reportSuspiciousActivity(userId, `Rate limit exceeded: ${actionType} (${tracker.actions.length}/${effectiveMax})`, ip);
            logger.warn(`Rate limit exceeded: ${key} (${tracker.actions.length}/${effectiveMax})`);
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
        
        // تحديث درجة التهديد
        this.updateThreatScore(userId, severity);
        
        logger.warn(`Suspicious activity detected: ${userId} - ${reason}`, { 
            severity, 
            warnings: activity.warnings,
            totalSeverity: activity.severity
        });
        
        // الحظر التلقائي
        const threatScore = this.threatScores.get(userId) || 0;
        if (threatScore >= 50) {
            this.banUser(userId, 'نشاط مشبوه خطير (تلقائي)', ip);
            return true;
        } else if (threatScore >= 30) {
            this.blockedUntil.set(`temp_${userId}`, Date.now() + 600000); // 10 دقائق
            logger.warn(`Temporary block for ${userId} (threat score: ${threatScore})`);
            return true;
        }
        
        return false;
    }
    
    calculateSeverity(reason) {
        if (reason.includes('aimbot') || reason.includes('godmode')) return 5;
        if (reason.includes('speedhack') || reason.includes('teleport')) return 4;
        if (reason.includes('spam') || reason.includes('flood')) return 2;
        if (reason.includes('fly') || reason.includes('noclip')) return 4;
        return 1;
    }
    
    updateThreatScore(userId, severity) {
        const current = this.threatScores.get(userId) || 0;
        const decay = 0.9; // تناقص تدريجي
        const newScore = current * decay + severity * 2;
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
                `UPDATE users SET 
                 is_banned = TRUE,
                 ban_reason = $1,
                 banned_until = $2,
                 banned_by = 'system'
                 WHERE id = $3`,
                [reason, new Date(Date.now() + this.banDuration), userId]
            );
            
            // تسجيل في قاعدة البيانات
            await db.query(
                `INSERT INTO transactions (id, user_id, type, amount, description, created_at)
                 VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP)`,
                [`tx_${Date.now()}_${uuidv4().slice(0, 8)}`, userId, 'ban', 0, `Banned: ${reason}`]
            );
            
        } catch (error) {
            logger.error('Error banning user in database:', { error: error.message, userId });
            monitoring.recordError('ban_user_error', error.message, 'anticheat');
        }
    }
    
    async isUserBanned(userId) {
        // التحقق من الذاكرة أولاً
        if (this.bannedUsers.has(userId)) return true;
        if (this.bannedCache.has(userId)) {
            const cached = this.bannedCache.get(userId);
            if (Date.now() - cached.timestamp < this.cacheTTL) {
                return true;
            }
            this.bannedCache.delete(userId);
        }
        
        try {
            const result = await db.query(
                'SELECT is_banned, banned_until FROM users WHERE id = $1',
                [userId]
            );
            if (result.rows.length > 0) {
                const user = result.rows[0];
                if (user.is_banned && user.banned_until && new Date(user.banned_until) > new Date()) {
                    this.bannedUsers.add(userId);
                    this.bannedCache.set(userId, { 
                        reason: 'Banned until ' + user.banned_until,
                        timestamp: Date.now() 
                    });
                    return true;
                }
            }
        } catch (error) {
            logger.error('Error checking ban status:', { error: error.message, userId });
            monitoring.recordError('ban_check_error', error.message, 'anticheat');
        }
        return false;
    }
    
    verifyAdminPassword(password) {
        const isValid = password === this.adminPassword;
        if (isValid) {
            this.penaltyMultiplier = 1;
        }
        return isValid;
    }
    
    updateAdminPassword(newPassword) {
        if (process.env.ADMIN_PASSWORD) {
            logger.warn('Admin password is set via environment variable, cannot change');
            return false;
        }
        this.adminPassword = newPassword;
        logger.info('Admin password updated');
        return true;
    }
    
    analyzeGameplayPattern(userId, actions) {
        const results = [];
        
        // تشغيل كل كاشفات الغش
        for (const [name, detector] of Object.entries(this.detectors)) {
            try {
                const detected = detector.detect(userId, actions);
                if (detected) {
                    results.push({ detector: name, ...detected });
                    this.reportSuspiciousActivity(userId, `${name}_detected`, null);
                }
            } catch (error) {
                logger.error(`Error in detector ${name}:`, { error: error.message });
            }
        }
        
        return results;
    }
    
    getStats() {
        return {
            bannedUsers: this.bannedUsers.size,
            bannedIPs: this.bannedIPs.size,
            suspiciousActivities: this.suspiciousActivity.size,
            activeTrackers: this.actionTracker.size,
            enabled: this.enabled,
            blocked: this.blockedUntil.size,
            globalBlockList: this.globalBlockList.size,
            threatScores: Array.from(this.threatScores.entries()).slice(0, 20)
        };
    }
    
    async unbanUser(userId) {
        this.bannedUsers.delete(userId);
        this.bannedCache.delete(userId);
        this.threatScores.delete(userId);
        
        try {
            await db.query(
                `UPDATE users SET 
                 is_banned = FALSE,
                 ban_reason = NULL,
                 banned_until = NULL
                 WHERE id = $1`,
                [userId]
            );
            logger.info(`User unbanned: ${userId}`);
            return true;
        } catch (error) {
            logger.error('Error unbanning user:', { error: error.message, userId });
            return false;
        }
    }
}

// ============================================
// 🔍 كاشفات الغش المتخصصة
// ============================================

class SpeedHackDetector {
    constructor() {
        this.history = new Map();
        this.threshold = 50; // وحدة سرعة قصوى
        this.sampleSize = 20;
    }
    
    detect(userId, actions) {
        if (!this.history.has(userId)) {
            this.history.set(userId, []);
        }
        
        const moves = actions.filter(a => a.type === 'move');
        if (moves.length < 2) return null;
        
        const history = this.history.get(userId);
        history.push(...moves);
        
        // الاحتفاظ بآخر العينات
        while (history.length > this.sampleSize) {
            history.shift();
        }
        
        if (history.length < 2) return null;
        
        let totalDistance = 0;
        let totalTime = 0;
        let maxSpeed = 0;
        
        for (let i = 1; i < history.length; i++) {
            const dx = history[i].x - history[i-1].x;
            const dz = history[i].z - history[i-1].z;
            const distance = Math.sqrt(dx*dx + dz*dz);
            const time = history[i].timestamp - history[i-1].timestamp;
            
            if (time > 0) {
                const speed = distance / time;
                totalDistance += distance;
                totalTime += time;
                maxSpeed = Math.max(maxSpeed, speed);
            }
        }
        
        const avgSpeed = totalTime > 0 ? totalDistance / totalTime : 0;
        
        if (maxSpeed > this.threshold * 1.5 || avgSpeed > this.threshold) {
            return {
                type: 'speedhack',
                confidence: Math.min(100, (maxSpeed / this.threshold) * 50),
                details: { maxSpeed, avgSpeed, totalDistance }
            };
        }
        
        return null;
    }
}

class AimbotDetector {
    constructor() {
        this.history = new Map();
        this.threshold = 0.95; // نسبة دقة أعلى من 95%
        this.sampleSize = 30;
    }
    
    detect(userId, actions) {
        const shots = actions.filter(a => a.type === 'shoot');
        const hits = actions.filter(a => a.type === 'hit');
        
        if (shots.length < 5) return null;
        
        const hitRate = hits.length / shots.length;
        
        // التحقق من توزيع الزوايا
        const angles = actions.filter(a => a.type === 'shoot' && a.angle !== undefined)
            .map(a => a.angle);
        
        let angleConsistency = 0;
        if (angles.length > 5) {
            const mean = angles.reduce((a, b) => a + b, 0) / angles.length;
            const variance = angles.reduce((a, b) => a + Math.pow(b - mean, 2), 0) / angles.length;
            angleConsistency = Math.sqrt(variance);
        }
        
        // نسبة دقة عالية جداً مع زوايا ثابتة = aimbot
        if (hitRate > this.threshold && angleConsistency < 0.1) {
            return {
                type: 'aimbot',
                confidence: Math.min(100, (hitRate - 0.9) * 500),
                details: { hitRate, angleConsistency, shots: shots.length, hits: hits.length }
            };
        }
        
        return null;
    }
}

class TeleportDetector {
    constructor() {
        this.history = new Map();
        this.threshold = 100; // مسافة قصوى للانتقال
        this.sampleSize = 10;
    }
    
    detect(userId, actions) {
        const moves = actions.filter(a => a.type === 'move');
        if (moves.length < 2) return null;
        
        const history = this.history.get(userId) || [];
        history.push(...moves);
        
        while (history.length > this.sampleSize) {
            history.shift();
        }
        this.history.set(userId, history);
        
        if (history.length < 2) return null;
        
        let teleports = 0;
        let maxDistance = 0;
        
        for (let i = 1; i < history.length; i++) {
            const dx = history[i].x - history[i-1].x;
            const dz = history[i].z - history[i-1].z;
            const distance = Math.sqrt(dx*dx + dz*dz);
            const time = history[i].timestamp - history[i-1].timestamp;
            
            maxDistance = Math.max(maxDistance, distance);
            
            // انتقال سريع غير طبيعي
            if (distance > this.threshold && time < 100) {
                teleports++;
            }
        }
        
        if (teleports >= 2) {
            return {
                type: 'teleport',
                confidence: Math.min(100, teleports * 30),
                details: { teleports, maxDistance }
            };
        }
        
        return null;
    }
}

class SpamDetector {
    constructor() {
        this.history = new Map();
        this.threshold = 20; // رسائل في الدقيقة
        this.sampleSize = 60;
    }
    
    detect(userId, actions) {
        const messages = actions.filter(a => a.type === 'chat');
        if (messages.length < 5) return null;
        
        const history = this.history.get(userId) || [];
        history.push(...messages);
        
        while (history.length > this.sampleSize) {
            history.shift();
        }
        this.history.set(userId, history);
        
        if (history.length < 10) return null;
        
        const now = Date.now();
        const oneMinuteAgo = now - 60000;
        const recent = history.filter(m => m.timestamp > oneMinuteAgo);
        
        if (recent.length > this.threshold) {
            return {
                type: 'spam',
                confidence: Math.min(100, (recent.length / this.threshold) * 50),
                details: { messages: recent.length, threshold: this.threshold }
            };
        }
        
        return null;
    }
}

class GodModeDetector {
    constructor() {
        this.history = new Map();
        this.threshold = 10; // عدد الضربات المستلمة دون ضرر
        this.sampleSize = 50;
    }
    
    detect(userId, actions) {
        const hits = actions.filter(a => a.type === 'hit_received');
        const damages = actions.filter(a => a.type === 'damage_taken');
        
        if (hits.length < 5) return null;
        
        const history = this.history.get(userId) || [];
        history.push(...hits);
        
        while (history.length > this.sampleSize) {
            history.shift();
        }
        this.history.set(userId, history);
        
        if (history.length < 10) return null;
        
        // حساب عدد الضربات التي لم تسبب ضرراً
        let noDamageHits = 0;
        for (const hit of history) {
            const damage = damages.find(d => d.timestamp === hit.timestamp);
            if (!damage || damage.amount === 0) {
                noDamageHits++;
            }
        }
        
        const ratio = noDamageHits / history.length;
        
        if (ratio > 0.8 && history.length > this.threshold) {
            return {
                type: 'godmode',
                confidence: Math.min(100, ratio * 80),
                details: { noDamageHits, totalHits: history.length, ratio }
            };
        }
        
        return null;
    }
}

const antiCheat = new AdvancedAntiCheat();

// ============================================
// 📦 التخزين المؤقت المتقدم
// ============================================
class AdvancedCacheManager {
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
        
        this.redisClient = null;
        this.useRedis = !!process.env.REDIS_URL;
        this.cacheHits = 0;
        this.cacheMisses = 0;
        this.cacheSize = 0;
        this.evictions = 0;
        this.maxCacheSize = 10000;
        
        if (this.useRedis) {
            this.initRedis();
        }
        
        this.cleanupInterval = setInterval(() => this.cleanup(), 60000);
        logger.info('Advanced Cache Manager initialized', { useRedis: this.useRedis });
    }
    
    async initRedis() {
        try {
            this.redisClient = createClient({
                url: process.env.REDIS_URL,
                socket: {
                    reconnectStrategy: (retries) => Math.min(retries * 100, 3000)
                }
            });
            
            this.redisClient.on('error', (err) => {
                logger.error('Redis cache error:', { error: err.message });
                this.useRedis = false;
            });
            
            await this.redisClient.connect();
            logger.info('✅ Redis connected for caching');
        } catch (error) {
            logger.error('Failed to connect to Redis for caching:', { error: error.message });
            this.useRedis = false;
        }
    }
    
    async get(key, cacheType = 'memory') {
        this.cacheSize = this.getTotalSize();
        
        // محاولة Redis أولاً
        if (this.useRedis && this.redisClient) {
            try {
                const value = await this.redisClient.get(`cache:${cacheType}:${key}`);
                if (value) {
                    this.cacheHits++;
                    return JSON.parse(value);
                }
            } catch (error) {
                logger.warn('Redis get failed, falling back to memory:', { error: error.message });
            }
        }
        
        // العودة إلى الذاكرة
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
        
        // تخزين في Redis
        if (this.useRedis && this.redisClient) {
            try {
                await this.redisClient.set(
                    `cache:${cacheType}:${key}`,
                    JSON.stringify(value),
                    { PX: ttlMs }
                );
            } catch (error) {
                logger.warn('Redis set failed:', { error: error.message });
            }
        }
        
        // تخزين في الذاكرة
        const cache = this.caches[cacheType];
        if (!cache) return false;
        
        // التحقق من الحجم
        if (this.cacheSize >= this.maxCacheSize) {
            this.evictOldest();
        }
        
        cache.set(key, {
            value,
            timestamp: Date.now(),
            ttl: ttlMs
        });
        
        this.cacheSize = this.getTotalSize();
        return true;
    }
    
    async delete(key, cacheType = 'memory') {
        if (this.useRedis && this.redisClient) {
            try {
                await this.redisClient.del(`cache:${cacheType}:${key}`);
            } catch (error) {
                logger.warn('Redis delete failed:', { error: error.message });
            }
        }
        
        const cache = this.caches[cacheType];
        if (cache) {
            cache.delete(key);
            this.cacheSize = this.getTotalSize();
        }
    }
    
    async clear(cacheType = null) {
        if (cacheType) {
            if (this.useRedis && this.redisClient) {
                try {
                    const keys = await this.redisClient.keys(`cache:${cacheType}:*`);
                    if (keys.length > 0) {
                        await this.redisClient.del(keys);
                    }
                } catch (error) {
                    logger.warn('Redis clear failed:', { error: error.message });
                }
            }
            this.caches[cacheType]?.clear();
        } else {
            if (this.useRedis && this.redisClient) {
                try {
                    const keys = await this.redisClient.keys('cache:*');
                    if (keys.length > 0) {
                        await this.redisClient.del(keys);
                    }
                } catch (error) {
                    logger.warn('Redis clear all failed:', { error: error.message });
                }
            }
            Object.values(this.caches).forEach(cache => cache.clear());
        }
        this.cacheSize = this.getTotalSize();
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
        for (const cache of Object.values(this.caches)) {
            size += cache.size;
        }
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
            useRedis: this.useRedis,
            caches: Object.fromEntries(
                Object.entries(this.caches).map(([type, cache]) => [type, cache.size])
            )
        };
    }
    
    async shutdown() {
        if (this.cleanupInterval) {
            clearInterval(this.cleanupInterval);
        }
        if (this.redisClient) {
            await this.redisClient.quit();
        }
    }
}

const cache = new AdvancedCacheManager();

// ============================================
// 🔌 معالج الطوابير المتقدم
// ============================================
class AdvancedQueueProcessor {
    constructor() {
        this.queues = {
            high: [],
            normal: [],
            low: [],
            dead: []
        };
        
        this.processing = {
            high: false,
            normal: false,
            low: false
        };
        
        this.maxConcurrent = {
            high: 20,
            normal: 10,
            low: 5
        };
        
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
        this.processedItems = new Set();
        this.idempotencyCache = new Map();
        
        logger.info('Advanced Queue Processor initialized');
    }
    
    async add(action, priority = 0, idempotencyKey = null) {
        return new Promise((resolve, reject) => {
            // التحقق من التكرار
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
            
            logger.debug(`Queue item added: ${item.id} (priority: ${priorityLevel})`);
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
            
            // تخزين النتيجة لمنع التكرار
            if (item.id) {
                this.idempotencyCache.set(item.id, {
                    result,
                    timestamp: Date.now()
                });
                
                // تنظيف الكاش القديم
                for (const [key, value] of this.idempotencyCache) {
                    if (Date.now() - value.timestamp > 60000) {
                        this.idempotencyCache.delete(key);
                    }
                }
            }
            
            const duration = Date.now() - item.startTime;
            this.processingTimes.push(duration);
            if (this.processingTimes.length > 1000) {
                this.processingTimes.shift();
            }
            this.stats.averageProcessingTime = 
                this.processingTimes.reduce((a, b) => a + b, 0) / this.processingTimes.length;
            
            logger.debug(`Item ${item.id} processed in ${duration}ms`);
            
        } catch (error) {
            this.stats.totalErrors++;
            item.status = 'failed';
            
            logger.error('Action processing error:', { 
                itemId: item.id, 
                error: error.message,
                retries: item.retries
            });
            
            if (item.retries < item.maxRetries) {
                // إعادة المحاولة بتأخير تصاعدي
                const delay = this.retryDelays[item.retries] || 5000;
                item.retries++;
                item.timestamp = Date.now() + delay;
                this.stats.retries++;
                
                setTimeout(() => {
                    this.queues[level].push(item);
                    this.process(level);
                }, delay);
                
                logger.info(`Retrying action ${item.id} (${item.retries}/${item.maxRetries}) after ${delay}ms`);
            } else {
                // نقل إلى طابور الموتى
                this.queues.dead.push({
                    ...item,
                    error: error.message,
                    failedAt: Date.now()
                });
                item.reject(error);
                logger.error(`Action ${item.id} moved to dead letter queue`);
            }
        }
    }
    
    getTotalSize() {
        return this.queues.high.length + this.queues.normal.length + 
               this.queues.low.length + this.queues.dead.length;
    }
    
    getStats() {
        return {
            queueLength: this.getTotalSize(),
            processing: {
                high: this.processing.high,
                normal: this.processing.normal,
                low: this.processing.low
            },
            maxConcurrent: this.maxConcurrent,
            ...this.stats,
            deadLetterCount: this.queues.dead.length,
            idempotencyCache: this.idempotencyCache.size
        };
    }
    
    async retryDeadLetter() {
        const items = [...this.queues.dead];
        this.queues.dead = [];
        
        for (const item of items) {
            await this.add(item.action, item.priority, item.id);
        }
        
        logger.info(`Retried ${items.length} items from dead letter queue`);
        return items.length;
    }
    
    clearDeadLetter() {
        const count = this.queues.dead.length;
        this.queues.dead = [];
        logger.info(`Cleared ${count} items from dead letter queue`);
        return count;
    }
}

const queueProcessor = new AdvancedQueueProcessor();

// ============================================
// 🎮 نظام ELO المتقدم
// ============================================
class AdvancedELOSystem {
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
        
        logger.info('Advanced ELO System initialized');
    }
    
    calculateNewELOs(playerA_ELO, playerB_ELO, playerA_won) {
        const expectedA = 1 / (1 + Math.pow(10, (playerB_ELO - playerA_ELO) / 400));
        const expectedB = 1 / (1 + Math.pow(10, (playerA_ELO - playerB_ELO) / 400));
        
        // K-Factor ديناميكي حسب مستوى اللاعب
        let kA = this.K_FACTOR;
        let kB = this.K_FACTOR;
        
        if (playerA_ELO > 2000) kA = 16;
        if (playerB_ELO > 2000) kB = 16;
        if (playerA_ELO < 1000) kA = 40;
        if (playerB_ELO < 1000) kB = 40;
        
        const newELO_A = playerA_ELO + kA * (playerA_won - expectedA);
        const newELO_B = playerB_ELO + kB * (1 - playerA_won - expectedB);
        
        return {
            newELO_A: Math.max(this.MIN_ELO, Math.min(this.MAX_ELO, Math.round(newELO_A))),
            newELO_B: Math.max(this.MIN_ELO, Math.min(this.MAX_ELO, Math.round(newELO_B)))
        };
    }
    
    getRank(elo) {
        let currentRank = this.ranks[0];
        for (const rank of this.ranks) {
            if (elo >= rank.minElo) {
                currentRank = rank;
            }
        }
        return currentRank;
    }
    
    getRankProgress(elo) {
        const currentRank = this.getRank(elo);
        const nextRank = this.ranks.find(r => r.minElo > currentRank.minElo);
        
        if (!nextRank) {
            return { current: currentRank, next: null, progress: 1 };
        }
        
        const progress = (elo - currentRank.minElo) / (nextRank.minElo - currentRank.minElo);
        return {
            current: currentRank,
            next: nextRank,
            progress: Math.min(1, Math.max(0, progress))
        };
    }
    
    async checkAchievements(userId, stats) {
        const achieved = [];
        const userAchievements = this.playerAchievements.get(userId) || new Set();
        
        // تحديث السلسلات
        this.updateStreaks(userId, stats);
        
        // التحقق من الإنجازات
        const checks = [
            { key: 'first_win', condition: stats.wins === 1 },
            { key: 'win_streak_3', condition: (this.winStreaks.get(userId) || 0) >= 3 },
            { key: 'win_streak_5', condition: (this.winStreaks.get(userId) || 0) >= 5 },
            { key: 'win_streak_10', condition: (this.winStreaks.get(userId) || 0) >= 10 },
            { key: 'kill_streak_5', condition: (this.killStreaks.get(userId) || 0) >= 5 },
            { key: 'kill_streak_10', condition: (this.killStreaks.get(userId) || 0) >= 10 },
            { key: 'perfect_game', condition: stats.kills >= 5 && stats.deaths === 0 },
            { key: 'veteran', condition: stats.gamesPlayed >= 100 },
            { key: 'legend', condition: stats.gamesPlayed >= 500 },
            { key: 'top_10', condition: stats.globalRank && stats.globalRank <= 10 },
            { key: 'top_1', condition: stats.globalRank && stats.globalRank === 1 }
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
        // تحديث سلسلة الانتصارات
        if (stats.won) {
            const current = this.winStreaks.get(userId) || 0;
            this.winStreaks.set(userId, current + 1);
        } else {
            this.winStreaks.set(userId, 0);
        }
        
        // تحديث سلسلة الإقصاءات
        if (stats.kills >= 5) {
            const current = this.killStreaks.get(userId) || 0;
            this.killStreaks.set(userId, current + 1);
        } else if (stats.kills === 0) {
            this.killStreaks.set(userId, 0);
        }
    }
    
    getAchievementReward(achievementKey) {
        return this.achievements[achievementKey]?.points || 0;
    }
    
    async processMatch(winnerId, loserId, stats) {
        // الحصول على التصنيف الحالي
        const winnerResult = await db.query('SELECT elo FROM users WHERE id = $1', [winnerId]);
        const loserResult = await db.query('SELECT elo FROM users WHERE id = $1', [loserId]);
        
        const winnerELO = winnerResult.rows[0]?.elo || this.DEFAULT_ELO;
        const loserELO = loserResult.rows[0]?.elo || this.DEFAULT_ELO;
        
        // حساب التصنيف الجديد
        const { newELO_A: newWinnerELO, newELO_B: newLoserELO } = this.calculateNewELOs(
            winnerELO, loserELO, true
        );
        
        // تحديث قاعدة البيانات
        await db.transaction(async (client) => {
            await client.query(
                `UPDATE users SET 
                 elo = $1,
                 wins = wins + 1,
                 games_played = games_played + 1,
                 total_rewards = total_rewards + $2,
                 last_game = CURRENT_TIMESTAMP
                 WHERE id = $3`,
                [newWinnerELO, stats.winReward || 0, winnerId]
            );
            
            await client.query(
                `UPDATE users SET 
                 elo = $1,
                 games_played = games_played + 1,
                 last_game = CURRENT_TIMESTAMP
                 WHERE id = $2`,
                [newLoserELO, loserId]
            );
            
            // تسجيل المعاملة
            await client.query(
                `INSERT INTO transactions (id, user_id, type, amount, description, created_at)
                 VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP)`,
                [
                    `tx_${Date.now()}_${uuidv4().slice(0, 8)}`,
                    winnerId,
                    'game_reward',
                    stats.winReward || 0,
                    `Win reward (ELO: ${winnerELO} → ${newWinnerELO})`
                ]
            );
        });
        
        // تحديث الكاش
        await cache.delete(winnerId, 'user');
        await cache.delete(loserId, 'user');
        await cache.delete('leaderboard_elo_10', 'leaderboard');
        
        // التحقق من الإنجازات
        const winnerAchievements = await this.checkAchievements(winnerId, {
            ...stats,
            won: true,
            globalRank: await this.getGlobalRank(winnerId)
        });
        
        const loserAchievements = await this.checkAchievements(loserId, {
            ...stats,
            won: false,
            globalRank: await this.getGlobalRank(loserId)
        });
        
        return {
            winner: {
                userId: winnerId,
                oldELO: winnerELO,
                newELO: newWinnerELO,
                change: newWinnerELO - winnerELO,
                rank: this.getRank(newWinnerELO),
                rankProgress: this.getRankProgress(newWinnerELO),
                achievements: winnerAchievements
            },
            loser: {
                userId: loserId,
                oldELO: loserELO,
                newELO: newLoserELO,
                change: newLoserELO - loserELO,
                rank: this.getRank(newLoserELO),
                rankProgress: this.getRankProgress(newLoserELO),
                achievements: loserAchievements
            }
        };
    }
    
    async getGlobalRank(userId) {
        try {
            const result = await db.query(
                `SELECT COUNT(*) + 1 as rank 
                 FROM users 
                 WHERE elo > (SELECT elo FROM users WHERE id = $1)`,
                [userId]
            );
            return result.rows[0]?.rank || 0;
        } catch (error) {
            return 0;
        }
    }
    
    getStats() {
        return {
            ranks: this.ranks,
            achievements: this.achievements,
            activeStreaks: {
                win: this.winStreaks.size,
                kill: this.killStreaks.size
            },
            totalAchievements: this.playerAchievements.size
        };
    }
}

const eloSystem = new AdvancedELOSystem();

// ============================================
// 🎯 تحميل إعدادات الخادم
// ============================================
let serverConfig = null;
let configVersion = 0;

async function loadServerConfig() {
    try {
        const cachedConfig = await cache.get('server_config', 'config');
        if (cachedConfig) {
            serverConfig = cachedConfig;
            return serverConfig;
        }
        
        const result = await db.query(
            "SELECT value, updated_at FROM server_config WHERE key = 'server_config'"
        );
        
        if (result.rows.length === 0) {
            serverConfig = getDefaultConfig();
            await saveServerConfig(serverConfig);
        } else {
            serverConfig = result.rows[0].value;
        }
        
        await cache.set('server_config', serverConfig, 'config');
        configVersion++;
        
        logger.info('Server config loaded successfully');
        return serverConfig;
    } catch (error) {
        logger.error('Error loading server config:', { error: error.message });
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
            tankSpeed: 0.25,
            tankRotationSpeed: 0.04,
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
            maxPlayersPerMatch: 16,
            leaderboardCacheTime: 30000,
            userCacheTime: 60000,
            reconnectTimeout: 30000,
            antiCheatEnabled: true,
            maxLoginAttempts: 5,
            lockTimeout: 30000,
            adminPassword: process.env.ADMIN_PASSWORD || 'Admin@2024#Battle',
            jwtSecret: process.env.JWT_SECRET || crypto.randomBytes(64).toString('hex'),
            enableAuditLog: true,
            rateLimitWindow: 60000,
            rateLimitMax: 100,
            maxConcurrentGames: 50,
            maxRooms: 50
        },
        appearance: {
            gameLogo: '/images/default/logo.png',
            backgroundImage: '/images/default/background.jpg',
            primaryColor: '#d4af37',
            secondaryColor: '#0a0f1a',
            accentColor: '#ff6b6b'
        },
        limits: {
            maxUsers: 1000,
            maxRooms: 50,
            maxGames: 20,
            maxConcurrentRequests: 1000,
            maxPlayerBalance: 1000000
        },
        monitoring: {
            enablePrometheus: true,
            enableDetailedLogging: true,
            healthCheckInterval: 15000,
            metricsRetention: 3600000,
            slowQueryThreshold: 1000
        },
        features: {
            enableRespawn: true,
            enableBoost: true,
            enableShield: true,
            enableKillFeed: true,
            enableSpectator: false,
            enableReplay: false
        }
    };
}

async function saveServerConfig(config) {
    try {
        await db.query(
            `INSERT INTO server_config (key, value, updated_at) 
             VALUES ('server_config', $1, CURRENT_TIMESTAMP) 
             ON CONFLICT (key) DO UPDATE SET 
             value = EXCLUDED.value, 
             updated_at = CURRENT_TIMESTAMP`,
            [config]
        );
        await cache.set('server_config', config, 'config');
        configVersion++;
        logger.info('Server config saved');
        return true;
    } catch (error) {
        logger.error('Error saving server config:', { error: error.message });
        return false;
    }
}

async function reloadServerConfig() {
    return await loadServerConfig();
}

// ============================================
// 🎮 إدارة الغرف المتقدمة
// ============================================
class AdvancedRoomManager {
    constructor() {
        this.rooms = new Map();
        this.activeGames = new Map();
        this.players = new Map();
        this.pendingReconnects = new Map();
        this.lock = new AdvancedDistributedLock();
        this.roomTypes = {
            beginner: { name: 'غرفة المبتدئين', icon: '🟢', minElo: 0, maxElo: 1200 },
            advanced: { name: 'غرفة المتقدمين', icon: '🟡', minElo: 1000, maxElo: 1800 },
            pro: { name: 'غرفة المحترفين', icon: '🔴', minElo: 1600, maxElo: 3000 }
        };
        
        this.initializeRooms();
        
        setInterval(() => {
            this.cleanupInactiveRooms();
            this.cleanupPendingReconnects();
        }, 60000);
        
        logger.info('Advanced Room Manager initialized');
    }
    
    async initializeRooms() {
        const lockKey = 'rooms_initialization';
        try {
            await this.lock.acquireLock(lockKey, 'system', 10000);
            
            const config = await loadServerConfig();
            const roomConfigs = config.rooms || {};
            
            this.rooms.clear();
            
            for (const [type, settings] of Object.entries(roomConfigs)) {
                if (!settings.enabled) continue;
                
                for (let i = 1; i <= settings.maxRooms; i++) {
                    const roomId = `${type}_room_${i}`;
                    const room = {
                        id: roomId,
                        type: type,
                        name: `${this.roomTypes[type]?.icon || '🏠'} ${this.roomTypes[type]?.name || type} ${i}`,
                        maxSeats: settings.maxSeats,
                        seatPrice: settings.seatPrice,
                        rewardMultiplier: settings.rewardMultiplier || 1,
                        players: [],
                        spectators: [],
                        status: 'waiting',
                        createdAt: Date.now(),
                        startTime: null,
                        gameRound: 0,
                        config: settings,
                        minElo: this.roomTypes[type]?.minElo || 0,
                        maxElo: this.roomTypes[type]?.maxElo || 3000,
                        stats: {
                            totalGames: 0,
                            totalKills: 0,
                            averageKills: 0
                        }
                    };
                    this.rooms.set(roomId, room);
                    
                    await db.query(
                        `INSERT INTO rooms (id, type, name, max_seats, seat_price, reward_multiplier, status, players, game_round)
                         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                         ON CONFLICT (id) DO UPDATE SET
                         type = EXCLUDED.type,
                         name = EXCLUDED.name,
                         max_seats = EXCLUDED.max_seats,
                         seat_price = EXCLUDED.seat_price,
                         reward_multiplier = EXCLUDED.reward_multiplier,
                         status = EXCLUDED.status,
                         players = EXCLUDED.players,
                         game_round = EXCLUDED.game_round`,
                        [roomId, type, room.name, room.maxSeats, room.seatPrice, room.rewardMultiplier, 'waiting', '[]', 0]
                    );
                }
            }
            
            logger.info(`Rooms initialized: ${this.rooms.size} rooms`);
            this.broadcastRoomsList();
            
            await this.lock.releaseLock(lockKey, 'system');
        } catch (error) {
            logger.error('Error initializing rooms:', { error: error.message });
            await this.lock.releaseLock(lockKey, 'system');
        }
    }
    
    async joinRoom(socket, userId, roomId) {
        const lockKey = `join_room_${userId}_${roomId}`;
        try {
            await this.lock.acquireLock(lockKey, userId, 5000);
            
            const room = this.rooms.get(roomId);
            if (!room) {
                socket.emit('join_room_error', { message: 'الغرفة غير موجودة' });
                await this.lock.releaseLock(lockKey, userId);
                return null;
            }
            
            if (room.status !== 'waiting') {
                socket.emit('join_room_error', { message: 'الغرفة مشغولة حالياً' });
                await this.lock.releaseLock(lockKey, userId);
                return null;
            }
            
            if (room.players.length >= room.maxSeats) {
                socket.emit('join_room_error', { message: 'الغرفة ممتلئة' });
                await this.lock.releaseLock(lockKey, userId);
                return null;
            }
            
            // التحقق من التصنيف
            const userData = await this.getUserData(userId);
            if (!userData) {
                socket.emit('join_room_error', { message: 'بيانات المستخدم غير موجودة' });
                await this.lock.releaseLock(lockKey, userId);
                return null;
            }
            
            const userElo = userData.elo || 1000;
            if (userElo < room.minElo || userElo > room.maxElo) {
                socket.emit('join_room_error', { 
                    message: `تصنيفك ${userElo} غير مناسب لهذه الغرفة (${room.minElo}-${room.maxElo})` 
                });
                await this.lock.releaseLock(lockKey, userId);
                return null;
            }
            
            if (userData.balance < room.seatPrice) {
                socket.emit('join_room_error', { 
                    message: `رصيد غير كافٍ. السعر: ${room.seatPrice}$` 
                });
                await this.lock.releaseLock(lockKey, userId);
                return null;
            }
            
            // خصم سعر المقعد
            await db.transaction(async (client) => {
                const result = await client.query(
                    'UPDATE users SET balance = balance - $1 WHERE id = $2 AND balance >= $1 RETURNING balance',
                    [room.seatPrice, userId]
                );
                if (result.rows.length === 0) {
                    throw new Error('Insufficient funds');
                }
                userData.balance = result.rows[0].balance;
                
                // تسجيل المعاملة
                await client.query(
                    `INSERT INTO transactions (id, user_id, type, amount, balance_before, balance_after, description)
                     VALUES ($1, $2, $3, $4, $5, $6, $7)`,
                    [
                        `tx_${Date.now()}_${uuidv4().slice(0, 8)}`,
                        userId,
                        'seat_purchase',
                        room.seatPrice,
                        userData.balance + room.seatPrice,
                        userData.balance,
                        `Join room ${room.name}`
                    ]
                );
            });
            
            const player = {
                userId,
                socketId: socket.id,
                username: userData.username || `لاعب_${userId.slice(0, 6)}`,
                elo: userData.elo || 1000,
                balance: userData.balance,
                health: 100,
                kills: 0,
                joinedAt: Date.now(),
                position: null,
                isSpectator: false
            };
            
            room.players.push(player);
            socket.join(roomId);
            
            this.players.set(userId, {
                socketId: socket.id,
                currentRoomId: roomId,
                userData
            });
            
            await db.query(
                'UPDATE rooms SET players = $1 WHERE id = $2',
                [JSON.stringify(room.players), roomId]
            );
            
            socket.emit('joined_room', {
                roomId,
                roomName: room.name,
                players: room.players.map(p => ({
                    userId: p.userId,
                    username: p.username,
                    elo: p.elo,
                    health: p.health
                })),
                balance: userData.balance,
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
            
            this.updateRoom(roomId);
            this.broadcastRoomsList();
            
            logger.info(`User ${userId} joined room ${roomId}`);
            
            await this.lock.releaseLock(lockKey, userId);
            return room;
            
        } catch (error) {
            logger.error('Error joining room:', { error: error.message, userId, roomId });
            socket.emit('join_room_error', { message: error.message });
            await this.lock.releaseLock(lockKey, userId);
            return null;
        }
    }
    
    async leaveRoom(socket, userId, roomId) {
        const lockKey = `leave_room_${userId}_${roomId}`;
        try {
            await this.lock.acquireLock(lockKey, userId, 5000);
            
            const room = this.rooms.get(roomId);
            if (!room) {
                await this.lock.releaseLock(lockKey, userId);
                return;
            }
            
            const playerIndex = room.players.findIndex(p => p.userId === userId);
            if (playerIndex === -1) {
                await this.lock.releaseLock(lockKey, userId);
                return;
            }
            
            const player = room.players[playerIndex];
            
            if (room.status === 'waiting') {
                const refund = player.paidAmount || room.seatPrice;
                await db.query(
                    'UPDATE users SET balance = balance + $1 WHERE id = $2',
                    [refund, userId]
                );
                socket.emit('balance_update', { 
                    balance: player.balance + refund,
                    message: `تم إعادة ${refund}$` 
                });
            }
            
            room.players.splice(playerIndex, 1);
            socket.leave(roomId);
            this.players.delete(userId);
            
            await db.query(
                'UPDATE rooms SET players = $1 WHERE id = $2',
                [JSON.stringify(room.players), roomId]
            );
            
            io.to(roomId).emit('player_left', {
                userId,
                playersCount: room.players.length,
                maxSeats: room.maxSeats
            });
            
            if (room.players.length === 0 && room.status === 'waiting') {
                this.resetRoom(roomId);
            }
            
            this.updateRoom(roomId);
            this.broadcastRoomsList();
            
            logger.info(`User ${userId} left room ${roomId}`);
            
            await this.lock.releaseLock(lockKey, userId);
        } catch (error) {
            logger.error('Error leaving room:', { error: error.message, userId, roomId });
            await this.lock.releaseLock(lockKey, userId);
        }
    }
    
    async resetRoom(roomId) {
        const lockKey = `reset_room_${roomId}`;
        try {
            await this.lock.acquireLock(lockKey, 'system', 10000);
            
            const oldRoom = this.rooms.get(roomId);
            if (!oldRoom) {
                await this.lock.releaseLock(lockKey, 'system');
                return;
            }
            
            if (this.activeGames.has(roomId)) {
                const game = this.activeGames.get(roomId);
                game.stop();
                this.activeGames.delete(roomId);
            }
            
            this.rooms.delete(roomId);
            await db.query('DELETE FROM rooms WHERE id = $1', [roomId]);
            
            const config = await loadServerConfig();
            const typeConfig = config.rooms[oldRoom.type];
            
            if (typeConfig && typeConfig.enabled) {
                let roomNumber = 1;
                let newRoomId = `${oldRoom.type}_room_${roomNumber}`;
                while (this.rooms.has(newRoomId)) {
                    roomNumber++;
                    newRoomId = `${oldRoom.type}_room_${roomNumber}`;
                }
                
                const newRoom = {
                    id: newRoomId,
                    type: oldRoom.type,
                    name: `${this.roomTypes[oldRoom.type]?.icon || '🏠'} ${this.roomTypes[oldRoom.type]?.name || oldRoom.type} ${roomNumber}`,
                    maxSeats: typeConfig.maxSeats,
                    seatPrice: typeConfig.seatPrice,
                    rewardMultiplier: typeConfig.rewardMultiplier || 1,
                    players: [],
                    spectators: [],
                    status: 'waiting',
                    createdAt: Date.now(),
                    startTime: null,
                    gameRound: (oldRoom.gameRound || 0) + 1,
                    config: typeConfig,
                    minElo: this.roomTypes[oldRoom.type]?.minElo || 0,
                    maxElo: this.roomTypes[oldRoom.type]?.maxElo || 3000,
                    stats: {
                        totalGames: oldRoom.stats?.totalGames || 0,
                        totalKills: oldRoom.stats?.totalKills || 0,
                        averageKills: oldRoom.stats?.averageKills || 0
                    }
                };
                
                this.rooms.set(newRoomId, newRoom);
                
                await db.query(
                    `INSERT INTO rooms (id, type, name, max_seats, seat_price, reward_multiplier, status, players, game_round)
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
                    [newRoomId, newRoom.type, newRoom.name, newRoom.maxSeats, newRoom.seatPrice, 
                     newRoom.rewardMultiplier, 'waiting', '[]', newRoom.gameRound]
                );
                
                io.emit('room_created', {
                    roomId: newRoomId,
                    name: newRoom.name,
                    maxSeats: newRoom.maxSeats,
                    seatPrice: newRoom.seatPrice,
                    gameRound: newRoom.gameRound
                });
                
                logger.info(`Room reset: ${oldRoom.name} -> ${newRoom.name} (Round ${newRoom.gameRound})`);
            }
            
            this.broadcastRoomsList();
            
            await this.lock.releaseLock(lockKey, 'system');
        } catch (error) {
            logger.error('Error resetting room:', { error: error.message, roomId });
            await this.lock.releaseLock(lockKey, 'system');
        }
    }
    
    async startGame(roomId) {
        const lockKey = `start_game_${roomId}`;
        try {
            await this.lock.acquireLock(lockKey, 'system', 10000);
            
            const room = this.rooms.get(roomId);
            if (!room || room.status !== 'waiting') {
                await this.lock.releaseLock(lockKey, 'system');
                return null;
            }
            
            const minPlayers = 2;
            if (room.players.length < minPlayers) {
                await this.lock.releaseLock(lockKey, 'system');
                return null;
            }
            
            room.status = 'active';
            room.startTime = Date.now();
            
            const game = new AdvancedGameEngine(roomId, room);
            game.start();
            this.activeGames.set(roomId, game);
            
            const gameStartData = {
                roomId,
                players: room.players.map(p => ({
                    userId: p.userId,
                    username: p.username,
                    elo: p.elo,
                    health: p.health
                })),
                startTime: room.startTime,
                gameRound: room.gameRound || 1,
                mode: 'battle_royale',
                totalPlayers: room.players.length,
                config: {
                    mapSize: 600,
                    boundaryLimit: 280,
                    bulletSpeed: 2.8,
                    bulletDamage: 25,
                    fireCooldown: 2000,
                    tankHealth: 100,
                    respawnTime: 5000
                }
            };
            
            for (const player of room.players) {
                const socket = io.sockets.sockets.get(player.socketId);
                if (socket) {
                    socket.emit('game_start', {
                        ...gameStartData,
                        yourId: player.userId,
                        position: this.getSpawnPosition(player.index || 0)
                    });
                }
            }
            
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
    
    async updateRoom(roomId) {
        const room = this.rooms.get(roomId);
        if (!room) return;
        
        io.to(roomId).emit('room_update', {
            players: room.players.map(p => ({
                userId: p.userId,
                username: p.username,
                elo: p.elo,
                health: p.health
            })),
            maxSeats: room.maxSeats,
            count: room.players.length,
            seatPrice: room.seatPrice,
            needed: room.maxSeats - room.players.length,
            status: room.status
        });
        
        if (room.players.length >= 2 && room.status === 'waiting') {
            await this.startGame(roomId);
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
                    needed: room.maxSeats - room.players.length,
                    startTime: room.startTime,
                    gameRound: room.gameRound || 0,
                    minElo: room.minElo,
                    maxElo: room.maxElo
                });
            }
        }
        io.emit('rooms_list', { rooms: roomsList });
    }
    
    broadcastLobbyInfo() {
        const totalPlayers = this.players.size;
        const activeRooms = Array.from(this.rooms.values()).filter(r => r.status === 'active').length;
        const waitingRooms = Array.from(this.rooms.values()).filter(r => r.status === 'waiting').length;
        
        io.emit('lobby_stats', {
            totalPlayers,
            activeRooms,
            waitingRooms,
            totalRooms: this.rooms.size,
            activeGames: this.activeGames.size,
            serverTime: Date.now(),
            maxPlayers: 1000,
            version: '11.0.0'
        });
    }
    
    cleanupInactiveRooms() {
        const now = Date.now();
        const timeout = 3600000;
        
        for (const [roomId, room] of this.rooms) {
            if (room.status === 'waiting' && 
                room.players.length === 0 && 
                now - room.createdAt > timeout) {
                this.rooms.delete(roomId);
                logger.info(`Removed inactive room: ${roomId}`);
            }
        }
    }
    
    cleanupPendingReconnects() {
        const now = Date.now();
        const timeout = 30000;
        
        for (const [key, data] of this.pendingReconnects) {
            if (now - data.timestamp > timeout) {
                this.pendingReconnects.delete(key);
                logger.debug(`Removed expired reconnect: ${key}`);
            }
        }
    }
    
    async getUserData(userId) {
        try {
            const cached = await cache.get(userId, 'user');
            if (cached) return cached;
            
            const result = await db.query('SELECT * FROM users WHERE id = $1', [userId]);
            if (result.rows.length === 0) {
                const newUser = {
                    id: userId,
                    telegram_id: userId,
                    username: `لاعب_${userId.slice(0, 6)}`,
                    balance: 100,
                    elo: 1000,
                    kills: 0,
                    wins: 0,
                    games_played: 0,
                    total_rewards: 0,
                    is_admin: userId === process.env.ADMIN_TELEGRAM_ID
                };
                
                await db.query(
                    `INSERT INTO users (id, telegram_id, username, balance, elo, is_admin, created_at, last_login)
                     VALUES ($1, $2, $3, $4, $5, $6, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
                    [newUser.id, newUser.telegram_id, newUser.username, newUser.balance, newUser.elo, newUser.is_admin]
                );
                
                await cache.set(userId, newUser, 'user');
                return newUser;
            }
            
            const user = result.rows[0];
            await cache.set(userId, user, 'user');
            return user;
            
        } catch (error) {
            logger.error('Error getting user data:', { error: error.message, userId });
            return null;
        }
    }
}

// ============================================
// 🎮 محرك اللعبة المتقدم
// ============================================
class AdvancedGameEngine {
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
        this.lastTick = Date.now();
        this.bulletId = 0;
        this.killRewards = new Map();
        this.eliminatedPlayers = new Set();
        this.gameRound = (room.gameRound || 0) + 1;
        this.config = null;
        this.stateHistory = [];
        this.maxHistorySize = 60;
        this.powerupSpawnTimer = 0;
        this.zoneShrink = {
            active: false,
            currentRadius: 280,
            targetRadius: 280,
            shrinkRate: 0,
            startTime: 0,
            duration: 0
        };
        
        this.initGame();
        logger.info(`Advanced GameEngine created for room ${roomId}`);
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
                speed: gameConfig.tankSpeed || 0.25,
                rotationSpeed: gameConfig.tankRotationSpeed || 0.04,
                active: true,
                powerups: []
            });
        }
        
        this.obstacles = this.generateObstacles();
        this.powerups = this.generatePowerups();
        
        this.aliveCount = this.tanks.size;
        
        for (const [userId, tank] of this.tanks) {
            const player = this.room.players.find(p => p.userId === userId);
            if (player) {
                player.position = { ...tank.position };
                player.health = tank.health;
            }
        }
        
        // بدء تقلص المنطقة
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
        const center = { x: 0, z: 0 };
        
        for (let i = 0; i < count; i++) {
            const angle = (i / count) * 2 * Math.PI + Math.random() * 0.1;
            const distance = radius * 0.4 + Math.random() * radius * 0.6;
            positions.push({
                x: center.x + Math.cos(angle) * distance,
                z: center.z + Math.sin(angle) * distance
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
                position: {
                    x: Math.cos(angle) * distance,
                    z: Math.sin(angle) * distance
                },
                radius: size,
                height: 2 + Math.random() * 4,
                type: Math.random() > 0.6 ? 'cover' : 'obstacle',
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
                position: {
                    x: Math.cos(angle) * distance,
                    z: Math.sin(angle) * distance
                },
                active: true,
                respawnTime: 10000,
                lastSpawn: Date.now()
            });
        }
        
        return powerups;
    }
    
    start() {
        this.tickInterval = setInterval(() => {
            const now = Date.now();
            const timeStep = (now - this.lastTick) / 1000;
            this.lastTick = now;
            this.update(Math.min(timeStep, 0.05));
        }, 50);
        
        setInterval(() => {
            io.to(this.roomId).emit('game_ping', { time: Date.now() });
        }, 5000);
        
        logger.info(`Game started for room ${this.roomId}`);
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
            
            if (bullet.life <= 0 || 
                Math.abs(bullet.position.x) > boundary ||
                Math.abs(bullet.position.z) > boundary) {
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
                powerup.position = {
                    x: Math.cos(angle) * distance,
                    z: Math.sin(angle) * distance
                };
            }
        }
    }
    
    updateZoneShrink(timeStep) {
        if (!this.zoneShrink.active) return;
        
        const elapsed = (Date.now() - this.zoneShrink.startTime) / 1000;
        const progress = Math.min(1, elapsed / (this.zoneShrink.duration / 1000));
        
        this.zoneShrink.currentRadius = this.zoneShrink.currentRadius - 
            (this.zoneShrink.shrinkRate * timeStep);
        
        this.zoneShrink.currentRadius = Math.max(
            this.zoneShrink.targetRadius,
            this.zoneShrink.currentRadius
        );
        
        // إرسال تحديث المنطقة
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
            // إعادة شحن الدرع
            if (tank.shield < tank.maxShield) {
                tank.shield = Math.min(tank.maxShield, tank.shield + 1 * timeStep);
            }
            
            // إعادة شحن التعزيز
            if (tank.boost < tank.maxBoost) {
                tank.boost = Math.min(tank.maxBoost, tank.boost + 5 * timeStep);
            }
            
            // ضرر المنطقة
            const distance = Math.sqrt(
                tank.position.x * tank.position.x + 
                tank.position.z * tank.position.z
            );
            
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
            
            // التحقق من الدبابات
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
            
            // التحقق من العقبات
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
                            
                            // تأثير تدمير العقبة
                            io.to(this.roomId).emit('obstacle_destroyed', {
                                id: obstacle.id,
                                position: obstacle.position
                            });
                        }
                        break;
                    }
                }
            }
            
            // التحقق من الـ Powerups
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
            
            if (bulletHit) {
                toRemove.push(i);
            }
        }
        
        for (const idx of toRemove.sort((a, b) => b - a)) {
            this.bullets.splice(idx, 1);
        }
    }
    
    handleDamage(shooterId, targetId, damage) {
        const target = this.tanks.get(targetId);
        if (!target || target.health <= 0 || this.eliminatedPlayers.has(targetId)) return;
        
        let actualDamage = damage;
        
        // الدرع يمتص الضرر
        if (target.shield > 0) {
            const shieldDamage = Math.min(target.shield, actualDamage);
            target.shield -= shieldDamage;
            actualDamage -= shieldDamage;
        }
        
        const newHealth = Math.max(0, target.health - actualDamage);
        target.health = newHealth;
        
        const targetPlayer = this.room.players.find(p => p.userId === targetId);
        if (targetPlayer) {
            targetPlayer.health = newHealth;
            targetPlayer.shield = target.shield;
        }
        
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
        
        const targetPlayer = this.room.players.find(p => p.userId === targetId);
        
        if (shooterId && shooterId !== targetId) {
            const reward = this.calculateKillReward(shooterId);
            this.killRewards.set(shooterId, (this.killRewards.get(shooterId) || 0) + reward);
            
            const killer = this.tanks.get(shooterId);
            if (killer) {
                killer.kills = (killer.kills || 0) + 1;
            }
            
            io.to(this.roomId).emit('player_eliminated', {
                targetId,
                killerId: shooterId,
                reward,
                targetName: target.name,
                killerName: this.tanks.get(shooterId)?.name || 'لاعب',
                aliveCount: this.aliveCount - 1,
                timestamp: Date.now()
            });
            
            this.killFeed.push({
                killer: this.tanks.get(shooterId)?.name || 'لاعب',
                target: target.name,
                timestamp: Date.now()
            });
            
            if (this.killFeed.length > 20) {
                this.killFeed.shift();
            }
            
            io.to(this.roomId).emit('kill_feed_update', {
                kills: this.killFeed.slice(-10)
            });
        }
        
        const targetSocketId = targetPlayer?.socketId;
        if (targetSocketId) {
            io.to(targetSocketId).emit('you_were_eliminated', {
                message: '💀 لقد تم تدمير دبابتك!',
                kills: this.killRewards.get(targetId) || 0,
                timestamp: Date.now()
            });
        }
        
        const respawnTime = this.config?.game?.respawnTime || 5000;
        setTimeout(() => {
            this.respawnPlayer(targetId);
        }, respawnTime);
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
            
            const player = this.room.players.find(p => p.userId === userId);
            if (player) {
                player.position = { ...pos };
                player.health = 100;
                player.shield = 50;
            }
            
            io.to(this.roomId).emit('player_respawned', {
                userId,
                position: tank.position,
                health: 100,
                shield: 50
            });
            
            io.to(userId).emit('respawn_success', {
                message: '✅ تم إعادة إحياء دبابتك!',
                position: tank.position
            });
            
            logger.debug(`Player ${userId} respawned`);
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
                setTimeout(() => {
                    tank.speed /= 1.3;
                }, 5000);
                message = `💨 زيادة السرعة`;
                break;
            case 'damage':
                // زيادة الضرر مؤقتاً
                message = `💥 زيادة الضرر`;
                break;
        }
        
        io.to(userId).emit('powerup_collected', {
            type: powerup.type,
            message: message
        });
        
        io.to(this.roomId).emit('powerup_used', {
            userId,
            type: powerup.type,
            position: powerup.position
        });
    }
    
    updateAliveCount() {
        let alive = 0;
        for (const [userId, tank] of this.tanks) {
            if (tank.health > 0 && !this.eliminatedPlayers.has(userId)) {
                alive++;
            }
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
            bullets: this.bullets.map(b => ({
                id: b.id,
                position: b.position,
                ownerId: b.ownerId
            })),
            obstacles: this.obstacles.filter(o => !o.destroyed),
            powerups: this.powerups.filter(p => p.active),
            aliveCount: this.aliveCount,
            zoneRadius: this.zoneShrink.currentRadius,
            timestamp: Date.now()
        };
        
        io.to(this.roomId).emit('game_state', state);
        
        this.stateHistory.push(state);
        if (this.stateHistory.length > this.maxHistorySize) {
            this.stateHistory.shift();
        }
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
        const config = await loadServerConfig();
        
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
            logger.error('Error saving match:', { error: error.message, roomId: this.roomId });
        }
        
        if (winnerId) {
            await this.distributeRewards(winnerId, winnerReward);
        }
        
        const result = {
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
        };
        
        io.to(this.roomId).emit('game_ended', result);
        
        this.room.status = 'finished';
        this.room.endTime = Date.now();
        
        logger.info(`Game ended in ${this.room.name}. Winner: ${winnerId || 'none'}`);
        monitoring.recordGameEnded(duration);
        
        setTimeout(() => {
            roomManager.resetRoom(this.roomId);
        }, 10000);
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
                    
                    const result = await client.query(
                        'SELECT elo, balance FROM users WHERE id = $1',
                        [player.userId]
                    );
                    
                    const currentELO = result.rows[0]?.elo || 1000;
                    const currentBalance = result.rows[0]?.balance || 0;
                    
                    let eloChange = 0;
                    if (isWinner) {
                        eloChange = 15 + Math.floor(kills / 2);
                    } else {
                        const performanceBonus = kills * 2;
                        eloChange = -5 + performanceBonus;
                    }
                    
                    const newELO = Math.max(1, currentELO + eloChange);
                    const newBalance = currentBalance + totalReward;
                    
                    await client.query(
                        `UPDATE users SET 
                         balance = $1,
                         elo = $2,
                         games_played = games_played + 1,
                         wins = wins + $3,
                         kills = kills + $4,
                         total_rewards = total_rewards + $5,
                         last_game = CURRENT_TIMESTAMP
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
                        killsReward,
                        timestamp: Date.now()
                    });
                }
            });
        } catch (error) {
            logger.error('Error distributing rewards:', { error: error.message, roomId: this.roomId });
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
    hsts: {
        maxAge: 31536000,
        includeSubDomains: true,
        preload: true
    }
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
    stream: {
        write: (message) => {
            logger.info('HTTP Request', { message: message.trim() });
        }
    }
}));

const limiter = rateLimit({
    windowMs: process.env.RATE_LIMIT_WINDOW || 60000,
    max: process.env.RATE_LIMIT_MAX || 100,
    message: { error: 'Too many requests, please try again later.' },
    standardHeaders: true,
    legacyHeaders: false,
    keyGenerator: (req) => {
        return req.ip || req.headers['x-forwarded-for'] || 'unknown';
    }
});
app.use('/api/', limiter);

// ============================================
// 📊 نقاط النهاية
// ============================================
app.get('/health', (req, res) => {
    const health = {
        status: 'ok',
        timestamp: new Date().toISOString(),
        uptime: monitoring.formatUptime(Math.floor((Date.now() - monitoring.startTime) / 1000)),
        version: '11.0.0',
        service: 'battle-tanks-royale',
        requestId: req.requestId,
        checks: {
            database: db.getHealth(),
            connections: monitoring.metrics.connections.active,
            memory: monitoring.metrics.system.memory,
            cpu: monitoring.metrics.system.cpu,
            worker: cluster.worker ? cluster.worker.id : 'main'
        }
    };
    
    const statusCode = health.checks.database.connected ? 200 : 503;
    res.status(statusCode).json(health);
});

app.get('/metrics', async (req, res) => {
    const metrics = monitoring.getPrometheusMetrics();
    const health = db.getHealth();
    
    let output = '# HELP battle_tanks_metrics Metrics for Battle Tanks Royale\n';
    output += '# TYPE battle_tanks_metrics gauge\n';
    output += `battle_tanks_worker ${cluster.worker ? cluster.worker.id : 0}\n`;
    
    for (const [key, value] of Object.entries(metrics)) {
        output += `battle_tanks_${key} ${value}\n`;
    }
    
    output += `battle_tanks_database_connected ${health.connected ? 1 : 0}\n`;
    output += `battle_tanks_database_pool_size ${health.poolSize || 0}\n`;
    output += `battle_tanks_cache_hit_rate ${cache.getStats().hitRate}\n`;
    output += `battle_tanks_queue_size ${queueProcessor.getStats().queueLength}\n`;
    
    res.set('Content-Type', 'text/plain');
    res.send(output);
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
    allowEIO3: true,
    path: '/socket.io',
    serveClient: false,
    connectTimeout: 45000
});

io.use(async (socket, next) => {
    try {
        const userId = socket.handshake.query.userId;
        const token = socket.handshake.auth.token;
        
        if (!userId) {
            return next(new Error('User ID required'));
        }
        
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
        socket.userData = await roomManager.getUserData(userId);
        socket.connectedAt = Date.now();
        
        next();
    } catch (error) {
        logger.error('Socket authentication error:', { error: error.message });
        next(new Error('Authentication failed'));
    }
});

io.on('connection', (socket) => {
    const userId = socket.userId;
    logger.info(`Socket connected: ${socket.id} for user ${userId}`);
    monitoring.recordConnection('connect', socket.handshake.address);
    
    roomManager.players.set(userId, {
        socketId: socket.id,
        userId,
        userData: socket.userData,
        currentRoomId: null,
        connectedAt: Date.now(),
        lastPing: Date.now()
    });
    
    socket.emit('connected', {
        userId,
        socketId: socket.id,
        timestamp: Date.now(),
        serverTime: Date.now(),
        version: '11.0.0'
    });
    
    roomManager.broadcastRoomsList();
    roomManager.broadcastLobbyInfo();
    
    // ============================================
    // 🎯 أحداث Socket.IO
    // ============================================
    
    socket.on('ping', (data) => {
        const latency = Date.now() - data.time;
        socket.emit('pong', {
            time: Date.now(),
            latency,
            serverTime: Date.now()
        });
        
        const player = roomManager.players.get(userId);
        if (player) {
            player.lastPing = Date.now();
            player.latency = latency;
        }
    });
    
    socket.on('join_room', async (data) => {
        const startTime = Date.now();
        try {
            const { roomId } = data;
            if (!roomId) {
                socket.emit('error', { message: 'Room ID required' });
                return;
            }
            
            if (!antiCheat.checkRateLimit(userId, 'join', socket.handshake.address)) {
                socket.emit('error', { message: 'Too many join attempts' });
                return;
            }
            
            const room = await roomManager.joinRoom(socket, userId, roomId);
            if (room) {
                monitoring.recordRequest(true, Date.now() - startTime);
            } else {
                monitoring.recordRequest(false, Date.now() - startTime);
            }
        } catch (error) {
            logger.error('Join room error:', { error: error.message, userId, roomId: data?.roomId });
            socket.emit('error', { message: error.message });
            monitoring.recordRequest(false, Date.now() - startTime);
            monitoring.recordError('join_room_error', error.message, 'socket');
        }
    });
    
    socket.on('leave_room', async (data) => {
        const startTime = Date.now();
        try {
            const { roomId } = data;
            if (!roomId) {
                socket.emit('error', { message: 'Room ID required' });
                return;
            }
            
            await roomManager.leaveRoom(socket, userId, roomId);
            monitoring.recordRequest(true, Date.now() - startTime);
        } catch (error) {
            logger.error('Leave room error:', { error: error.message, userId, roomId: data?.roomId });
            socket.emit('error', { message: error.message });
            monitoring.recordRequest(false, Date.now() - startTime);
            monitoring.recordError('leave_room_error', error.message, 'socket');
        }
    });
    
    socket.on('player_move', (data) => {
        const player = roomManager.players.get(userId);
        if (!player?.currentRoomId) return;
        
        if (!antiCheat.checkRateLimit(userId, 'move', socket.handshake.address)) {
            return;
        }
        
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
            boost: data.boost || false,
            timestamp: Date.now()
        });
    });
    
    socket.on('player_shoot', (data) => {
        const player = roomManager.players.get(userId);
        if (!player?.currentRoomId) return;
        
        if (!antiCheat.checkRateLimit(userId, 'shoot', socket.handshake.address)) {
            return;
        }
        
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
    
    socket.on('get_leaderboard', async (data) => {
        try {
            const limit = data?.limit || 100;
            const type = data?.type || 'elo';
            
            const result = await db.query(
                `SELECT id, username, elo, kills, wins, games_played, balance, total_rewards 
                 FROM users 
                 ORDER BY ${type === 'elo' ? 'elo DESC' : type === 'wins' ? 'wins DESC' : type === 'kills' ? 'kills DESC' : 'total_rewards DESC'} 
                 LIMIT $1`,
                [limit]
            );
            
            const leaderboard = result.rows.map((user, index) => ({
                ...user,
                rank: index + 1,
                eloRank: eloSystem.getRank(user.elo || 1000)
            }));
            
            socket.emit('leaderboard_update', { leaderboard });
        } catch (error) {
            logger.error('Get leaderboard error:', { error: error.message, userId });
            socket.emit('error', { message: 'Failed to get leaderboard' });
        }
    });
    
    socket.on('get_stats', async () => {
        try {
            const user = await roomManager.getUserData(userId);
            if (user) {
                const rank = eloSystem.getRank(user.elo || 1000);
                socket.emit('stats_update', {
                    ...user,
                    rank,
                    rankProgress: eloSystem.getRankProgress(user.elo || 1000),
                    globalRank: await eloSystem.getGlobalRank(userId)
                });
            }
        } catch (error) {
            logger.error('Get stats error:', { error: error.message, userId });
            socket.emit('error', { message: 'Failed to get stats' });
        }
    });
    
    socket.on('disconnect', () => {
        logger.info(`Socket disconnected: ${socket.id} for user ${userId}`);
        monitoring.recordConnection('disconnect');
        
        const player = roomManager.players.get(userId);
        if (player && player.currentRoomId) {
            const reconnectKey = `reconnect_${userId}_${Date.now()}`;
            roomManager.pendingReconnects.set(reconnectKey, {
                userId,
                roomId: player.currentRoomId,
                socketId: socket.id,
                timestamp: Date.now()
            });
            
            setTimeout(async () => {
                const pending = roomManager.pendingReconnects.get(reconnectKey);
                if (pending && pending.socketId === socket.id) {
                    roomManager.pendingReconnects.delete(reconnectKey);
                    await roomManager.leaveRoom(socket, userId, player.currentRoomId);
                    logger.info(`User ${userId} auto-left room after disconnect timeout`);
                }
            }, 30000);
        }
        
        roomManager.players.delete(userId);
        roomManager.broadcastLobbyInfo();
    });
});

// ============================================
// 🚀 بدء تشغيل الخادم
// ============================================
const PORT = process.env.PORT || 3000;

async function startServer() {
    try {
        await db.connect();
        await initializeDatabase();
        await loadServerConfig();
        
        const roomManager = new AdvancedRoomManager();
        
        setInterval(() => {
            roomManager.broadcastLobbyInfo();
            roomManager.broadcastRoomsList();
        }, 5000);
        
        setInterval(async () => {
            try {
                await db.healthCheck();
            } catch (error) {
                logger.error('Health check error:', { error: error.message });
            }
        }, 15000);
        
        server.listen(PORT, () => {
            console.log(`
╔══════════════════════════════════════════════════════════════════════════════╗
║                                                                              ║
║     🎮 BATTLE TANKS ROYALE - الإصدار النهائي المتكامل v11.0.0 🎮         ║
║                                                                              ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  📡 Server: http://localhost:${PORT}
║  🗄️ Database: PostgreSQL ✅ Connected
║  ⚡ WebSocket: Ready
║  🎯 Mode: Battle Royale (Free-for-All)
║  🔄 Cluster: ${isProduction ? `✅ ${numCPUs} workers` : '❌ Single thread'}
║                                                                              ║
║  🛡️ Anti-Cheat: ${antiCheat.enabled ? '✅ ENABLED' : '❌ DISABLED'}
║  🔒 Lock System: ${lockSystem ? '✅ ACTIVE' : '❌ INACTIVE'}
║  📊 Queue System: ✅ ACTIVE
║  📈 Monitoring: ✅ ACTIVE
║  💾 Cache: ${cache.useRedis ? '✅ REDIS' : '✅ MEMORY'}
║                                                                              ║
║  🏠 Rooms: ${roomManager.rooms.size} available
║  👥 Players: ${roomManager.players.size} online
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
║     - POST /api/admin/config
║     - POST /api/admin/maintenance
║                                                                              ║
║  🔄 Database Auto-Reconnect: ✅ ENABLED
║  ⏱️  Reconnect Delay: Exponential (5s - 60s)
║  🔁 Max Reconnect Attempts: Unlimited
║  ⚡ Circuit Breaker: ✅ ENABLED
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
    
    server.close(() => {
        logger.info('HTTP server closed');
    });
    
    io.close(() => {
        logger.info('WebSocket server closed');
    });
    
    await db.shutdown();
    await cache.shutdown();
    await lockSystem.shutdown();
    monitoring.stop();
    
    process.exit(0);
}

process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

startServer();

module.exports = {
    app,
    server,
    io,
    db,
    monitoring,
    lockSystem,
    antiCheat,
    cache,
    queueProcessor,
    roomManager,
    eloSystem,
    loadServerConfig,
    saveServerConfig
};
