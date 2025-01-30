import chalk from "chalk";
import highlight from "cli-highlight";
import fs from "fs";
import path from "path";

export class Logger {
    private LOG: boolean = true;
    private DEBUG: boolean = true;
    private ERROR: boolean = true;
    private WARN: boolean = true;
    private INFO: boolean = true;
    private logFilePath: string;
    private useStdout: boolean;

    constructor(options?: any) {
        if (typeof options === "boolean" && !options) {
            this.LOG = false;
            this.DEBUG = false;
            this.ERROR = false;
        }
        this.logFilePath = path.join(process.cwd(), "db-logs.txt");
        this.useStdout = options?.useStdout ?? true;
    }

    public log(...messages: any[]): void {
        if (this.LOG) {
            this.writeToFile(
                ...this.formatMessages(messages, chalk.blueBright, "LOG"),
            );
            this.writeToStdout(
                ...this.formatMessages(messages, chalk.blueBright, "LOG"),
            );
        }
    }

    public error(...messages: any[]): void {
        if (this.ERROR) {
            this.writeToFile(
                ...this.formatMessages(messages, chalk.redBright, "ERROR"),
            );
            this.writeToStderr(
                ...this.formatMessages(messages, chalk.redBright, "ERROR"),
            );
        }
    }

    public debug(...messages: any[]): void {
        if (this.DEBUG) {
            this.writeToFile(
                ...this.formatMessages(messages, chalk.greenBright, "DEBUG"),
            );
            this.writeToStdout(
                ...this.formatMessages(messages, chalk.greenBright, "DEBUG"),
            );
        }
    }

    public warn(...messages: any[]): void {
        if (this.WARN) {
            this.writeToFile(
                ...this.formatMessages(messages, chalk.yellowBright, "WARN"),
            );
            this.writeToStdout(
                ...this.formatMessages(messages, chalk.yellowBright, "WARN"),
            );
        }
    }

    public info(...messages: any[]): void {
        if (this.INFO) {
            this.writeToFile(
                ...this.formatMessages(messages, chalk.cyanBright, "INFO"),
            );
            this.writeToStdout(
                ...this.formatMessages(messages, chalk.cyanBright, "INFO"),
            );
        }
    }

    public logSql(sql: string): void {
        console.log(highlight(sql, { language: "sql", ignoreIllegals: true }));
    }

    private formatMessages(
        messages: any[],
        colorFn: chalk.Chalk,
        prefix: string,
    ): string[] {
        const time = new Date().toISOString();
        const prefixFormatted = colorFn(`[${prefix}]`);
        const timeFormatted = chalk.gray(`[${time}]`);
        const contentFormatted = messages.map((message) =>
            this.formatMessage(message),
        );

        return [`${prefixFormatted} ${timeFormatted}`, ...contentFormatted];
    }

    private formatMessage(message: any): string {
        if (typeof message === "string" || typeof message === "number") {
            return chalk.yellow(message.toString());
        } else if (message instanceof Error) {
            return chalk.red(message.stack || message.message);
        } else if (typeof message === "object") {
            return chalk.magenta(JSON.stringify(message, null, 2));
        } else {
            return chalk.white(String(message));
        }
    }

    private writeToFile(...messages: string[]): void {
        const logMessage = messages.join(" ") + "\n";
        fs.appendFileSync(this.logFilePath, logMessage);
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
