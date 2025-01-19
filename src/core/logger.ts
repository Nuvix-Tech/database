import chalk from 'chalk';
import { highlight } from 'cli-highlight';

export class Logger {
  private LOG: boolean = true;
  private DEBUG: boolean = true;
  private ERROR: boolean = true;

  constructor(private highlightOptions = { language: 'typescript', ignoreIllegals: true }) { }

  public log(...messages: any[]): void {
    if (this.LOG) {
      const formattedMessage = this.formatMessages(messages, chalk.blueBright, 'LOG');
      console.log(formattedMessage);
    }
  }

  public error(...messages: any[]): void {
    if (this.ERROR) {
      const formattedMessage = this.formatMessages(messages, chalk.redBright, 'ERROR');
      console.error(formattedMessage);
    }
  }

  public debug(...messages: any[]): void {
    if (this.DEBUG) {
      const formattedMessage = this.formatMessages(messages, chalk.greenBright, 'DEBUG');
      console.debug(formattedMessage);
    }
  }

  private formatMessages(messages: any[], colorFn: chalk.Chalk, prefix: string): string {
    const time = new Date().toLocaleTimeString();
    const prefixFormatted = colorFn(`[${prefix}]`);
    const timeFormatted = chalk.gray(`[${time}]`);
    const contentFormatted = messages.map(message => {
      if (typeof message === 'object') {
        return highlight(JSON.stringify(message, null, 2), this.highlightOptions);
      } else {
        return message.toString();
      }
    }).join(' ');

    return `${prefixFormatted} ${timeFormatted} ${contentFormatted}`;
  }
}
