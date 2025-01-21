import chalk from 'chalk';

export class Logger {
  private LOG: boolean = true;
  private DEBUG: boolean = true;
  private ERROR: boolean = true;
  private WARN: boolean = true;
  private INFO: boolean = true;

  constructor(options?: any) {
    if (typeof options === "boolean" && !options) {
      this.LOG = false;
      this.DEBUG = false;
      this.ERROR = false;
    }
  }

  public log(...messages: any[]): void {
    if (this.LOG) {
      console.log(...this.formatMessages(messages, chalk.blueBright, 'LOG'));
    }
  }

  public error(...messages: any[]): void {
    if (this.ERROR) {
      console.error(...this.formatMessages(messages, chalk.redBright, 'ERROR'));
    }
  }

  public debug(...messages: any[]): void {
    if (this.DEBUG) {
      console.debug(...this.formatMessages(messages, chalk.greenBright, 'DEBUG'));
    }
  }

  public warn(...messages: any[]): void {
    if (this.WARN) {
      console.warn(...this.formatMessages(messages, chalk.yellowBright, 'WARN'));
    }
  }

  public info(...messages: any[]): void {
    if (this.INFO) {
      console.info(...this.formatMessages(messages, chalk.cyanBright, 'INFO'));
    }
  }

  private formatMessages(messages: any[], colorFn: chalk.Chalk, prefix: string): any[] {
    const time = new Date().toLocaleTimeString();
    const prefixFormatted = colorFn(`[${prefix}]`);
    const timeFormatted = chalk.gray(`[${time}]`);
    const contentFormatted = messages.map(message => this.formatMessage(message));

    return [`${prefixFormatted} ${timeFormatted}`, ...contentFormatted];
  }

  private formatMessage(message: any): any {
    if (typeof message === 'string' || typeof message === 'number') {
      return chalk.yellow(message.toString());
    } else {
      return message;
    }
  }
}
