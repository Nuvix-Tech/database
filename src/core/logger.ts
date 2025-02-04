import fs from "fs";
import path from "path";
import chalk from "chalk";
import highlight from "cli-highlight";

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
    useStdout?: boolean;
    logFilePath?: string;
}

/**
 * Logger class for structured logging with configurable levels
 */
export class Logger {
    private logEnabled: boolean;
    private debugEnabled: boolean;
    private errorEnabled: boolean;
    private warnEnabled: boolean;
    private infoEnabled: boolean;
    private sqlEnabled: boolean;
    private logFilePath: string;
    private useStdout: boolean;

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
            logFilePath: path.join(process.cwd(), "db-logs.txt"),
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
            };
        }

        const finalOptions = { ...defaultOptions, ...(options as object) };

        this.logEnabled = finalOptions.log!;
        this.debugEnabled = finalOptions.debug!;
        this.errorEnabled = finalOptions.error!;
        this.warnEnabled = finalOptions.warn!;
        this.infoEnabled = finalOptions.info!;
        this.sqlEnabled = finalOptions.sql!;
        this.useStdout = finalOptions.useStdout!;
        this.logFilePath = finalOptions.logFilePath!;
    }

    public log(...messages: unknown[]): void {
        if (this.logEnabled) {
            this.output("LOG", chalk.blueBright, messages);
        }
    }

    public error(...messages: unknown[]): void {
        if (this.errorEnabled) {
            this.output("ERROR", chalk.redBright, messages, true);
        }
    }

    public debug(...messages: unknown[]): void {
        if (this.debugEnabled) {
            this.output("DEBUG", chalk.greenBright, messages);
        }
    }

    public warn(...messages: unknown[]): void {
        if (this.warnEnabled) {
            this.output("WARN", chalk.yellowBright, messages);
        }
    }

    public info(...messages: unknown[]): void {
        if (this.infoEnabled) {
            this.output("INFO", chalk.cyanBright, messages);
        }
    }

    public logSql(sql: string): void {
        if (this.sqlEnabled) {
            const highlightedSQL = highlight(sql, {
                language: "sql",
                ignoreIllegals: true,
            });
            this.writeToStdout(chalk.gray(`[SQL]`), highlightedSQL);
        }
    }

    private output(
        level: string,
        colorFn: chalk.Chalk,
        messages: unknown[],
        isError: boolean = false,
    ): void {
        const formattedMessages = this.formatMessages(level, colorFn, messages);
        this.writeToFile(formattedMessages);
        isError
            ? this.writeToStderr(formattedMessages)
            : this.writeToStdout(formattedMessages);
    }

    private formatMessages(
        level: string,
        colorFn: chalk.Chalk,
        messages: unknown[],
    ): string {
        const time = chalk.gray(`[${new Date().toISOString()}]`);
        const prefix = colorFn(`[${level}]`);
        const formattedContent = messages.map(this.formatMessage).join(" ");
        return `${prefix} ${time} ${formattedContent}`;
    }

    private formatMessage(message: unknown): string {
        if (typeof message === "string" || typeof message === "number") {
            return chalk.yellow(message.toString());
        } else if (message instanceof Error) {
            return chalk.red(message.stack || message.message);
        } else if (typeof message === "object" && message !== null) {
            return chalk.magenta(JSON.stringify(message, null, 2));
        }
        return chalk.white(String(message));
    }

    private writeToFile(message: string): void {
        try {
            fs.appendFileSync(this.logFilePath, message + "\n", "utf8");
        } catch (err) {
            console.error("Failed to write to log file:", err);
        }
    }

    private writeToStdout(...messages: string[]): void {
        if (this.useStdout) {
            process.stdout.write(messages.join(" ") + "\n");
        }
    }

    private writeToStderr(...messages: string[]): void {
        if (this.useStdout) {
            process.stderr.write(messages.join(" ") + "\n");
        }
    }
}
