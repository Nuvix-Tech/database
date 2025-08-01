import { EventEmitter } from "stream";
import { EventsEnum } from "./enums.js";


export class Database extends EventEmitter<Record<EventsEnum, any>> {

    public static INTERNAL_ATTRIBUTES: string[] = [];
    public static INTERNAL_INDEXES: string[] = [];
    public static METADATA: string = '_metadata';

    constructor() {
        super();
    }

}
