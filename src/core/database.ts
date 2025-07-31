import { EventEmitter } from "stream";
import { EventsEnum } from "./enums.js";


export class Database extends EventEmitter<Record<EventsEnum, any>> {

    constructor() {
        super();
    }

}
