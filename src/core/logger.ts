import fs from "fs";
import path from "path";
import chalk from "chalk";
import highlight from "cli-highlight";
import { promises as fsPromises } from "fs";

/**
 * Log level enum in order of increasing verbosity
 */
export enum LogLevel {
    NONE = 0,
    ERROR = 1,
    WARN = 2,
    INFO = 3,
    SQL = 4,
    LOG = 5,
    DEBUG = 6,
}

/**
 * Logger configuration options
 */
export interface LoggerOptions {
    log?: boolean;
    debug?: boolean;
    error?: boolean;
    warn?: boolean;
    info?: boolean;
    sql?: boolean;
    writeLogFile?: boolean;
    useStdout?: boolean;
    useStderr?: boolean;
    logFilePath?: string;
    logLevel?: LogLevel;
    maxLogFileSize?: number; // in bytes
    maxLogFiles?: number;
    asyncFileLogging?: boolean;
    includeTimestamp?: boolean;
}

/**
 * Context object for structured logging
 */
export interface LogContext {
    [key: string]: any;
    requestId?: string;
}

/**
 * Logger class for structured logging with configurable levels
 */
export class Logger {
    private static logEnabled: boolean;
    private static debugEnabled: boolean;
    private static errorEnabled: boolean;
    private static warnEnabled: boolean;
    private static infoEnabled: boolean;
    private static sqlEnabled: boolean;
    private static logFilePath: string;
    private static useStdout: boolean;
    private static useStderr: boolean;
    private static logLevel: LogLevel;
    private static maxLogFileSize: number;
    private static maxLogFiles: number;
    private static asyncFileLogging: boolean;
    private static includeTimestamp: boolean;
    private static fileLoggingEnabled: boolean = false;
    private static writeQueue: Promise<void> = Promise.resolve();
    private context: LogContext = {};

    constructor(options?: LoggerOptions | boolean) {
        // Default options
        const defaultOptions: LoggerOptions = {
            log: true,
            debug: true,
            error: true,
            warn: true,
            info: true,
            sql: true,
            useStdout: true,
            useStderr: true,
            logFilePath: path.join(process.cwd(), "db.log"),
            logLevel: LogLevel.INFO,
            maxLogFileSize: 10 * 1024 * 1024, // 10 MB
            maxLogFiles: 5,
            asyncFileLogging: true,
            includeTimestamp: true,
            writeLogFile: false,
        };

        // Handle boolean option (disable all logging)
        if (typeof options === "boolean" && !options) {
            options = {
                log: false,
                debug: false,
                error: false,
                warn: false,
                info: false,
                sql: false,
                logLevel: LogLevel.NONE,
            };
        }

        const finalOptions = { ...defaultOptions, ...(options as object) };

        Logger.logEnabled = finalOptions.log!;
        Logger.debugEnabled = finalOptions.debug!;
        Logger.errorEnabled = finalOptions.error!;
        Logger.warnEnabled = finalOptions.warn!;
        Logger.infoEnabled = finalOptions.info!;
        Logger.sqlEnabled = finalOptions.sql!;
        Logger.useStdout = finalOptions.useStdout!;
        Logger.useStderr = finalOptions.useStderr!;
        Logger.logFilePath = finalOptions.logFilePath!;
        Logger.logLevel = finalOptions.logLevel!;
        Logger.maxLogFileSize = finalOptions.maxLogFileSize!;
        Logger.maxLogFiles = finalOptions.maxLogFiles!;
        Logger.asyncFileLogging = finalOptions.asyncFileLogging!;
        Logger.includeTimestamp = finalOptions.includeTimestamp!;
        Logger.fileLoggingEnabled = finalOptions.writeLogFile!;

        // Initialize log directory if it doesn't exist
        const logDir = path.dirname(Logger.logFilePath);
        if (!fs.existsSync(logDir)) {
            fs.mkdirSync(logDir, { recursive: true });
        }
    }

    /**
     * Create a child logger with additional context
     */
    public child(context: LogContext): Logger {
        const childLogger = new Logger();
        childLogger.context = { ...this.context, ...context };
        return childLogger;
    }

    /**
     * Create a logger with request tracking
     */
    public withRequestId(requestId: string = crypto.randomUUID()): Logger {
        return this.child({ requestId });
    }

    public log(...messages: unknown[]): void {
        if (Logger.logEnabled && Logger.logLevel >= LogLevel.LOG) {
            this.output("LOG", chalk.blueBright, messages);
        }
    }

    public error(...messages: unknown[]): void {
        if (Logger.errorEnabled && Logger.logLevel >= LogLevel.ERROR) {
            this.output("ERROR", chalk.redBright, messages, true);
        }
    }

    public debug(...messages: unknown[]): void {
        if (Logger.debugEnabled && Logger.logLevel >= LogLevel.DEBUG) {
            this.output("DEBUG", chalk.greenBright, messages);
        }
    }

    public warn(...messages: unknown[]): void {
        if (Logger.warnEnabled && Logger.logLevel >= LogLevel.WARN) {
            this.output("WARN", chalk.yellowBright, messages);
        }
    }

    public info(...messages: unknown[]): void {
        if (Logger.infoEnabled && Logger.logLevel >= LogLevel.INFO) {
            this.output("INFO", chalk.cyanBright, messages);
        }
    }

    public logSql(sql: string): void {
        if (Logger.sqlEnabled && Logger.logLevel >= LogLevel.SQL) {
            const highlightedSQL = highlight(sql, {
                language: "sql",
                ignoreIllegals: true,
            });
            this.writeToStdout(chalk.gray(`[SQL]`), highlightedSQL);
            this.writeToFile([{ sql, type: "SQL" }]);
        }
    }

    private output(
        level: string,
        colorFn: chalk.Chalk,
        messages: unknown[],
        isError: boolean = false,
    ): void {
        const formattedMessages = this.formatMessages(level, colorFn, messages);
        if (Logger.fileLoggingEnabled) {
            this.writeToFile(
                messages.map((msg) => ({
                    ...this.context,
                    level,
                    message: msg,
                })),
            );
        }

        isError
            ? this.writeToStderr(formattedMessages)
            : this.writeToStdout(formattedMessages);
    }

    private formatMessages(
        level: string,
        colorFn: chalk.Chalk,
        messages: unknown[],
    ): string {
        const time = Logger.includeTimestamp
            ? chalk.gray(`[${new Date().toISOString()}]`)
            : "";
        const prefix = colorFn(`[${level}]`);
        const requestIdStr = this.context.requestId
            ? chalk.magenta(`[${this.context.requestId}]`)
            : "";
        const formattedContent = messages
            .map((m) => this.formatMessage(m))
            .join(" ");
        return `${prefix} ${time} ${requestIdStr} ${formattedContent}`.trim();
    }

    private formatMessage(
        message: unknown,
        _chalk: boolean = true,
    ): string | any {
        if (typeof message === "string" || typeof message === "number") {
            return _chalk
                ? chalk.yellow(message.toString())
                : message.toString();
        } else if (message instanceof Error) {
            return _chalk
                ? chalk.red(message.stack || message.message)
                : message.stack || message.message;
        } else if (typeof message === "object" && message !== null) {
            return _chalk
                ? chalk.magenta(JSON.stringify(message, null, 2))
                : message;
        }
        return chalk.white(String(message));
    }

    private async rotateLogFilesIfNeeded(): Promise<void> {
        try {
            // Check if file exists and if it exceeds the maximum size
            if (fs.existsSync(Logger.logFilePath)) {
                const stats = fs.statSync(Logger.logFilePath);
                if (stats.size >= Logger.maxLogFileSize) {
                    // Rotate the log files
                    for (let i = Logger.maxLogFiles - 1; i >= 0; i--) {
                        const oldPath =
                            i === 0
                                ? Logger.logFilePath
                                : `${Logger.logFilePath}.${i}`;
                        const newPath = `${Logger.logFilePath}.${i + 1}`;

                        if (fs.existsSync(oldPath)) {
                            if (i === Logger.maxLogFiles - 1) {
                                // Delete the oldest log file
                                fs.unlinkSync(oldPath);
                            } else {
                                // Rename the file
                                fs.renameSync(oldPath, newPath);
                            }
                        }
                    }
                }
            }
        } catch (err) {
            console.error("Failed to rotate log files:", err);
        }
    }

    private writeToFile(messages: unknown[]): void {
        if (Logger.asyncFileLogging) {
            Logger.writeQueue = Logger.writeQueue.then(() =>
                this.writeToFileAsync(messages),
            );
        } else {
            this.writeToFileSync(messages);
        }
    }

    private writeToFileSync(messages: unknown[]): void {
        try {
            this.rotateLogFilesIfNeeded();

            const logEntries = messages.map((message) => ({
                timestamp: new Date().toISOString(),
                ...this.context,
                message: this.formatMessage(message, false),
            }));
            const logContent =
                logEntries.map((entry) => JSON.stringify(entry)).join("\n") +
                "\n";
            fs.appendFileSync(Logger.logFilePath, logContent, "utf8");
        } catch (err) {
            console.error("Failed to write to log file:", err);
        }
    }

    private async writeToFileAsync(messages: unknown[]): Promise<void> {
        try {
            await this.rotateLogFilesIfNeeded();

            const logEntries = messages.map((message) => ({
                timestamp: new Date().toISOString(),
                ...this.context,
                message: this.formatMessage(message, false),
            }));
            const logContent =
                logEntries.map((entry) => JSON.stringify(entry)).join("\n") +
                "\n";
            await fsPromises.appendFile(Logger.logFilePath, logContent, "utf8");
        } catch (err) {
            console.error("Failed to write to log file:", err);
        }
    }

    private writeToStdout(...messages: string[]): void {
        if (Logger.useStdout) {
            process.stdout.write(messages.join(" ") + "\n");
        }
    }

    private writeToStderr(...messages: string[]): void {
        if (Logger.useStderr) {
            process.stderr.write(messages.join(" ") + "\n");
        }
    }

    /**
     * Set the global log level
     */
    public static setLogLevel(level: LogLevel): void {
        Logger.logLevel = level;
        Logger.errorEnabled = level >= LogLevel.ERROR;
        Logger.warnEnabled = level >= LogLevel.WARN;
        Logger.infoEnabled = level >= LogLevel.INFO;
        Logger.sqlEnabled = level >= LogLevel.SQL;
        Logger.logEnabled = level >= LogLevel.LOG;
        Logger.debugEnabled = level >= LogLevel.DEBUG;
    }
}
