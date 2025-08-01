import { EventEmitter } from "stream";
import { EventsEnum, PermissionEnum } from "./enums.js";


export class Database extends EventEmitter<Record<EventsEnum, any>> {
    public static METADATA = '_metadata' as const;
    public static INTERNAL_ATTRIBUTES: string[] = [];
    public static INTERNAL_INDEXES: string[] = [];
    public static PERMISSIONS: PermissionEnum[] = [
        PermissionEnum.Create,
        PermissionEnum.Read,
        PermissionEnum.Update,
        PermissionEnum.Delete,
    ];
    public static ARRAY_INDEX_LENGTH: number = 1000; // TODO 

    constructor() {
        super();
    }

}
