import { pino, Logger as PinoLogger } from 'pino';

export class Logger {
    private logger: PinoLogger;

    constructor() {
        this.logger = pino();
    }

    info(message: string, ...args: any[]) {
        this.logger.info(message, ...args);
    }

    warn(message: string, ...args: any[]) {
        this.logger.warn(message, ...args);
    }

    error(message: string, ...args: any[]) {
        this.logger.error(message, ...args);
    }

    debug(message: string, ...args: any[]) {
        this.logger.debug(message, ...args);
    }

    static warn (message: string, ...args: any[]) {
        const logger = new Logger();
        logger.warn(message, ...args);
    }
}
