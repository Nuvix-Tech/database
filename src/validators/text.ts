import { Validator } from "./interface.js";

export class Text implements Validator {
  public static readonly NUMBERS = [
    "0",
    "1",
    "2",
    "3",
    "4",
    "5",
    "6",
    "7",
    "8",
    "9",
  ];
  public static readonly ALPHABET_UPPER = [
    "A",
    "B",
    "C",
    "D",
    "E",
    "F",
    "G",
    "H",
    "I",
    "J",
    "K",
    "L",
    "M",
    "N",
    "O",
    "P",
    "Q",
    "R",
    "S",
    "T",
    "U",
    "V",
    "W",
    "X",
    "Y",
    "Z",
  ];
  public static readonly ALPHABET_LOWER = [
    "a",
    "b",
    "c",
    "d",
    "e",
    "f",
    "g",
    "h",
    "i",
    "j",
    "k",
    "l",
    "m",
    "n",
    "o",
    "p",
    "q",
    "r",
    "s",
    "t",
    "u",
    "v",
    "w",
    "x",
    "y",
    "z",
  ];

  protected length: number;
  protected min: number;
  protected allowList: string[];

  /**
   * Text constructor.
   *
   * Validate text with maximum length. Use length = 0 for unlimited length.
   * Optionally, provide allowList characters array to only allow specific characters.
   *
   * @param length - Maximum length of the text
   * @param min - Minimum length of the text
   * @param allowList - Allowed characters
   */
  constructor(length: number, min: number = 1, allowList: string[] = []) {
    this.length = length;
    this.min = min;
    this.allowList = allowList;
  }

  public get $description() {
    let message = "Value must be a valid string";

    if (this.min === this.length) {
      message += ` and exactly ${this.length} chars`;
    } else {
      if (this.min) {
        message += ` and at least ${this.min} chars`;
      }

      if (this.length) {
        message += ` and no longer than ${this.length} chars`;
      }
    }

    if (this.allowList.length > 0) {
      message += ` and only consist of '${this.allowList.join(", ")}' chars`;
    }

    return message;
  }

  public $valid(value: any): boolean {
    if (typeof value !== "string") {
      return false;
    }

    if (value.length < this.min) {
      return false;
    }

    if (value.length > this.length && this.length !== 0) {
      return false;
    }

    if (this.allowList.length > 0) {
      for (const char of value) {
        if (!this.allowList.includes(char)) {
          return false;
        }
      }
    }

    return true;
  }
}
