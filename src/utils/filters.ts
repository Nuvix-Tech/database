import { Doc } from "@core/doc.js";
import { FiltersEnum } from "@core/enums.js";
import { Filter } from "@core/types.js";
import { Logger } from "./logger.js";
import { DatabaseException } from "@errors/base.js";
import { DateTime } from "./date-time.js";

const jsonFilter: Filter = {
    encode(value: any): string | null {
        if (typeof value === 'string') return value;
        if (value === null || value === undefined) return null;

        try {
            return JSON.stringify(value);
        } catch (error) {
            Logger.error('JSON encode error:', error);
            return null;
        }
    },

    decode(value: any): any {
        if (typeof value !== 'string') return value;

        try {
            const parsed = JSON.parse(value);
            if (!parsed) return parsed;

            if (Array.isArray(parsed)) {
                return parsed.map((item) => {
                    if (item && typeof item === 'object' && '$id' in item) {
                        return new Doc(item);
                    }
                    return item;
                });
            }

            if (parsed && typeof parsed === 'object' && '$id' in parsed) {
                return new Doc(parsed);
            }

            return parsed;
        } catch (error) {
            Logger.error('JSON decode error:', error);
            return value;
        }
    },
}

const datetimeFilter: Filter<string | number | Date | null, string | null> = {
    encode(value: string | Date | number | null): string | null {
        if (!value) return null;

        try {
            const date = new Date(value);
            if (isNaN(date.getTime())) {
                throw new DatabaseException(`Invalid date input: ${value}`);
            }
            return DateTime.formatForDB(date);
        } catch (error) {
            Logger.error('Failed to encode datetime:', {
                value,
                error,
            });
            return null;
        }
    },

    decode(value: string | null): Date | null {
        if (!value) return null;

        try {
            return DateTime.fromDB(value);
        } catch (error) {
            Logger.error('Failed to decode datetime:', {
                value,
                error,
            });
            return null;
        }
    },
};

export const filters = {
    [FiltersEnum.Json]: jsonFilter,
    [FiltersEnum.Datetime]: datetimeFilter,
}
