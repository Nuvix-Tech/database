import chalk from 'chalk';
import { highlight } from 'cli-highlight';

export class Logger {
  private LOG: boolean = true;
  private DEBUG: boolean = true;
  private ERROR: boolean = true;

  constructor(private highlightOptions = { language: 'typescript', ignoreIllegals: true }) { }

  public log(message: any): void {
    if (this.LOG) {
      const formattedMessage = this.formatMessage(message, chalk.blueBright, 'LOG');
      console.log(formattedMessage);
    }
  }

  public error(message: any): void {
    if (this.ERROR) {
      const formattedMessage = this.formatMessage(message, chalk.redBright, 'ERROR');
      console.error(formattedMessage);
    }
  }

  public debug(message: any): void {
    if (this.DEBUG) {
      message = message ? message : ''
      const formattedMessage = this.formatMessage(message, chalk.greenBright, 'DEBUG');
      console.debug(formattedMessage);
    }
  }

  private formatMessage(message: any, colorFn: chalk.Chalk, prefix: string): string {
    const time = new Date().toISOString();
    const prefixFormatted = colorFn(`[${prefix}]`);
    const timeFormatted = chalk.gray(`[${time}]`);
    let contentFormatted;

    if (typeof message === 'object') {
      contentFormatted = highlight(JSON.stringify(message, null, 2), this.highlightOptions);
    } else {
      contentFormatted = message.toString();
    }

    return `${prefixFormatted} ${timeFormatted} ${contentFormatted}`;
  }
}
