import { pino, Logger as PinoLogger, LoggerOptions } from 'pino';

export class Logger {
    private logger: PinoLogger;
    private static staticLogger: PinoLogger;

    constructor(options?: LoggerOptions) {
        this.logger = pino({
            ...options
        });
    }

    private static getStaticLogger(): PinoLogger {
        if (!this.staticLogger) {
            this.staticLogger = pino({
            });
        }
        return this.staticLogger;
    }

    info(message: string, ...args: any[]): void {
        this.logger.info(message, ...args);
    }

    warn(message: string, ...args: any[]): void {
        this.logger.warn(message, ...args);
    }

    error(message: string, ...args: any[]): void {
        this.logger.error(message, ...args);
    }

    debug(message: string, ...args: any[]): void {
        this.logger.debug(message, ...args);
    }

    static info(message: string, ...args: any[]): void {
        this.getStaticLogger().info(message, ...args);
    }

    static warn(message: string, ...args: any[]): void {
        this.getStaticLogger().warn(message, ...args);
    }

    static error(message: string, ...args: any[]): void {
        this.getStaticLogger().error(message, ...args);
    }

    static debug(message: string, ...args: any[]): void {
        this.getStaticLogger().debug(message, ...args);
    }
}
