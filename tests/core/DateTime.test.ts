import { DateTime } from '../../src/core/date-time';
import { DatabaseException } from '../../src/errors/base';

describe('DateTime', () => {
    const RealDate = Date;

    function mockDate(isoDate: string) {
        // @ts-ignore
        global.Date = class extends RealDate {
            constructor(...args: any[]) {
                if (args.length) {
                    // @ts-ignore
                    super(...args);
                } else {
                    super(isoDate);
                }
            }

            static now() {
                return new RealDate(isoDate).getTime();
            }
        };
    }

    afterEach(() => {
        global.Date = RealDate;
    });

    describe('now()', () => {
        it('should return the current date and time in DB format', () => {
            const specificDate = '2023-10-26T10:30:00.123Z';
            mockDate(specificDate);
            // The expected value should match the local time interpretation of the UTC string
            const date = new Date(specificDate);
            const expected = `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')} ${String(date.getHours()).padStart(2, '0')}:${String(date.getMinutes()).padStart(2, '0')}:${String(date.getSeconds()).padStart(2, '0')}.${String(date.getMilliseconds()).padStart(3, '0')}`;
            expect(DateTime.now()).toBe(expected);
        });
    });

    describe('format()', () => {
        it('should format a Date object to DB format string', () => {
            const date = new RealDate('2023-10-26T10:30:00.123Z');
            const expected = `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')} ${String(date.getHours()).padStart(2, '0')}:${String(date.getMinutes()).padStart(2, '0')}:${String(date.getSeconds()).padStart(2, '0')}.${String(date.getMilliseconds()).padStart(3, '0')}`;
            expect(DateTime.format(date, DateTime.FORMAT_DB)).toBe(expected);
        });

        it('should format a Date object to ISO string (default TZ format)', () => {
            const date = new RealDate('2023-10-26T10:30:00.123Z');
            const expected = '2023-10-26T10:30:00.123Z';
            expect(DateTime.format(date)).toBe(expected);
            expect(DateTime.format(date, DateTime.FORMAT_TZ)).toBe(expected);
        });
    });

    describe('addSeconds()', () => {
        it('should add seconds to a Date object and return formatted string', () => {
            const date = new RealDate('2023-10-26T10:30:00.000Z');
            const secondsToAdd = 65;
            const newDate = new RealDate(date.getTime() + secondsToAdd * 1000);
            const expected = `${newDate.getFullYear()}-${String(newDate.getMonth() + 1).padStart(2, '0')}-${String(newDate.getDate()).padStart(2, '0')} ${String(newDate.getHours()).padStart(2, '0')}:${String(newDate.getMinutes()).padStart(2, '0')}:${String(newDate.getSeconds()).padStart(2, '0')}.${String(newDate.getMilliseconds()).padStart(3, '0')}`;
            expect(DateTime.addSeconds(date, secondsToAdd)).toBe(expected);
        });

        it('should subtract seconds when a negative number is provided', () => {
            const date = new RealDate('2023-10-26T10:30:00.000Z');
            const secondsToSubtract = -65;
            const newDate = new RealDate(date.getTime() + secondsToSubtract * 1000);
            const expected = `${newDate.getFullYear()}-${String(newDate.getMonth() + 1).padStart(2, '0')}-${String(newDate.getDate()).padStart(2, '0')} ${String(newDate.getHours()).padStart(2, '0')}:${String(newDate.getMinutes()).padStart(2, '0')}:${String(newDate.getSeconds()).padStart(2, '0')}.${String(newDate.getMilliseconds()).padStart(3, '0')}`;
            expect(DateTime.addSeconds(date, secondsToSubtract)).toBe(expected);
        });

        it('should throw DatabaseException for invalid seconds input', () => {
            const date = new RealDate();
            expect(() => DateTime.addSeconds(date, Infinity)).toThrow(DatabaseException);
            expect(() => DateTime.addSeconds(date, NaN)).toThrow(DatabaseException);
        });
    });

    describe('setTimezone()', () => {
        it('should convert a datetime string to local timezone format (mocked as UTC for test consistency)', () => {
            const utcDateTime = '2023-10-26T10:30:00.000Z';
            const originalGetTimezoneOffset = RealDate.prototype.getTimezoneOffset;
            RealDate.prototype.getTimezoneOffset = () => 0; // Mock to UTC

            const date = new Date(utcDateTime);
            const offset = 0;
            const localDate = new Date(date.getTime() - offset * 60000);
            const expected = `${localDate.getFullYear()}-${String(localDate.getMonth() + 1).padStart(2, '0')}-${String(localDate.getDate()).padStart(2, '0')} ${String(localDate.getHours()).padStart(2, '0')}:${String(localDate.getMinutes()).padStart(2, '0')}:${String(localDate.getSeconds()).padStart(2, '0')}.${String(localDate.getMilliseconds()).padStart(3, '0')}`;
            expect(DateTime.setTimezone(utcDateTime)).toBe(expected);

            RealDate.prototype.getTimezoneOffset = originalGetTimezoneOffset; // Restore
        });

        it('should throw DatabaseException for invalid datetime input', () => {
            expect(() => DateTime.setTimezone('')).toThrow();
        });
    });

    describe('formatTz()', () => {
        it('should format a DB datetime string with timezone information', () => {
            const dbFormat = '2023-10-26 10:30:00.123';
            // The expected value is the ISO string of the local time interpreted from dbFormat
            const date = new Date(dbFormat);
            const expected = date.toISOString();
            expect(DateTime.formatTz(dbFormat)).toBe(expected);
        });

        it('should return null for null input', () => {
            expect(DateTime.formatTz(null)).toBeNull();
        });

        it('should return null for invalid date strings', () => {
            expect(DateTime.formatTz('0000-00-00 00:00:00')).toBeNull();
            expect(DateTime.formatTz('invalid-date')).toBe('invalid-date'); // Returns original if parsing fails
        });
    });
});
