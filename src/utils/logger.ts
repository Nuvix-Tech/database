import fs from 'fs';
import path from 'path';
import chalk from 'chalk';

type LogLevel = 'error' | 'warn' | 'info' | 'debug';

const LEVEL_COLORS: Record<LogLevel, typeof chalk> = {
    error: chalk.red,
    warn: chalk.yellow,
    info: chalk.green,
    debug: chalk.blue,
};

interface LoggerOptions {
    level?: LogLevel;
    context?: string;
    timestamp?: boolean;
    logFilePath?: string;
    maxFileSize?: number; // bytes
}

type Serializer = (obj: any) => string;

export class Logger {
    private level: LogLevel;
    private context?: string;
    private timestamp: boolean;
    private logFilePath?: string;
    private maxFileSize: number;

    private writeStream?: fs.WriteStream;
    private logBuffer: string[] = [];
    private flushIntervalMs = 100;
    private flushTimer?: NodeJS.Timeout;

    private serializers = new Map<Function, Serializer>();

    private static staticInstance?: Logger;

    constructor(options?: LoggerOptions) {
        this.level = options?.level ?? 'info';
        this.context = options?.context;
        this.timestamp = options?.timestamp ?? true;
        this.logFilePath = options?.logFilePath;
        this.maxFileSize = options?.maxFileSize ?? 5 * 1024 * 1024; // default 5MB

        if (this.logFilePath) {
            this.initWriteStream();
        }

        // Register default error serializer
        this.registerSerializer(Error, (err) => {
            return `${err.name}: ${err.message}\n${err.stack}`;
        });
    }

    private initWriteStream() {
        try {
            if (!fs.existsSync(path.dirname(this.logFilePath!))) {
                fs.mkdirSync(path.dirname(this.logFilePath!), { recursive: true });
            }
            this.writeStream = fs.createWriteStream(this.logFilePath!, { flags: 'a' });
            this.flushTimer = setInterval(() => this.flushBuffer(), this.flushIntervalMs);
        } catch (err) {
            console.error("Logger: Failed to initialize write stream", err);
        }
    }

    private rotateFileIfNeeded() {
        if (!this.writeStream || !this.logFilePath) return;
        try {
            const stats = fs.statSync(this.logFilePath);
            if (stats.size >= this.maxFileSize) {
                this.writeStream.close();
                const rotatedPath =
                    this.logFilePath +
                    '.' +
                    new Date().toISOString().replace(/[:.]/g, '-');
                fs.renameSync(this.logFilePath, rotatedPath);
                this.initWriteStream();
            }
        } catch {
            // Ignore stat errors (e.g., file not found)
        }
    }

    private flushBuffer() {
        if (!this.writeStream || this.logBuffer.length === 0) return;
        const data = this.logBuffer.join('\n') + '\n';
        this.writeStream.write(data);
        this.logBuffer.length = 0;
    }

    private shouldLog(level: LogLevel): boolean {
        const levels: LogLevel[] = ['error', 'warn', 'info', 'debug'];
        return levels.indexOf(level) <= levels.indexOf(this.level);
    }

    private serializeArg(arg: any): string {
        if (arg === null || arg === undefined) return String(arg);
        for (const [type, serializer] of this.serializers) {
            if (arg instanceof type) {
                try {
                    return serializer(arg);
                } catch {
                    return '[Serializer error]';
                }
            }
        }
        if (typeof arg === 'object') {
            try {
                return JSON.stringify(arg, null, 2);
            } catch {
                return '[Unserializable object]';
            }
        }
        return String(arg);
    }

    private formatMessage(level: LogLevel, message: string, ...args: any[]) {
        const color = LEVEL_COLORS[level] || chalk.white;
        const timeStr = this.timestamp ? chalk.gray(new Date().toISOString()) + ' ' : '';
        const contextStr = this.context ? chalk.magenta(`[${this.context}] `) : '';
        const levelStr = color(level.toUpperCase().padEnd(5));
        const formattedArgs = args.length ? ' ' + args.map(a => this.serializeArg(a)).join(' ') : '';
        return `${timeStr}${levelStr} ${contextStr}${message}${formattedArgs}`;
    }

    private log(level: LogLevel, message: string, ...args: any[]) {
        if (!this.shouldLog(level)) return;

        const output = this.formatMessage(level, message, ...args);

        // Console output
        if (level === 'error' || level === 'warn') {
            console.error(output);
        } else {
            console.log(output);
        }

        // File output (no color codes)
        if (this.writeStream) {
            const plainText = output.replace(/\x1b\[[0-9;]*m/g, '');
            this.logBuffer.push(plainText);
            if (this.logBuffer.length > 1000) {
                this.flushBuffer();
            }
            this.rotateFileIfNeeded();
        }
    }

    // Instance methods
    info(message: string, ...args: any[]) {
        this.log('info', message, ...args);
    }
    warn(message: string, ...args: any[]) {
        this.log('warn', message, ...args);
    }
    error(message: string, ...args: any[]) {
        this.log('error', message, ...args);
    }
    debug(message: string, ...args: any[]) {
        this.log('debug', message, ...args);
    }

    registerSerializer(type: Function, serializer: Serializer) {
        this.serializers.set(type, serializer);
    }

    async close() {
        if (this.flushTimer) {
            clearInterval(this.flushTimer);
            this.flushTimer = undefined;
        }
        this.flushBuffer();
        if (this.writeStream) {
            await new Promise<void>((resolve) => this.writeStream!.end(resolve));
        }
    }

    // Static singleton methods for quick usage
    private static getStaticInstance(): Logger {
        if (!this.staticInstance) {
            this.staticInstance = new Logger();
        }
        return this.staticInstance;
    }

    static info(message: string, ...args: any[]) {
        this.getStaticInstance().info(message, ...args);
    }
    static warn(message: string, ...args: any[]) {
        this.getStaticInstance().warn(message, ...args);
    }
    static error(message: string, ...args: any[]) {
        this.getStaticInstance().error(message, ...args);
    }
    static debug(message: string, ...args: any[]) {
        this.getStaticInstance().debug(message, ...args);
    }

    static registerSerializer(type: Function, serializer: Serializer) {
        this.getStaticInstance().registerSerializer(type, serializer);
    }

    static async close() {
        await this.getStaticInstance().close();
    }
}
