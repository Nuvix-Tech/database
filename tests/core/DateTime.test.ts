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
            const expected = '2023-10-26 10:30:00.123'; // Adjust based on how your FORMAT_DB works with local vs UTC
            expect(DateTime.now()).toBe(expected);
        });
    });

    describe('format()', () => {
        it('should format a Date object to DB format string', () => {
            const date = new RealDate('2023-10-26T10:30:00.123Z');
            const expected = '2023-10-26 10:30:00.123';
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
            const expected = '2023-10-26 10:31:05.000';
            expect(DateTime.addSeconds(date, secondsToAdd)).toBe(expected);
        });

        it('should subtract seconds when a negative number is provided', () => {
            const date = new RealDate('2023-10-26T10:30:00.000Z');
            const secondsToSubtract = -65;
            const expected = '2023-10-26 10:28:55.000';
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
            // This test is tricky because it depends on the local timezone of the test runner.
            // For consistency, we'll test with a UTC-like string and expect a UTC-like output,
            // assuming the setTimezone logic correctly handles offsets.
            const utcDateTime = '2023-10-26T10:30:00.000Z';
            // If setTimezone correctly removes the Z and assumes local, then formatting it as DB should match.
            // The actual result depends on the machine's timezone. Here we assume UTC for the test.
            // To make this test robust, one might need to mock getTimezoneOffset.
            const originalGetTimezoneOffset = RealDate.prototype.getTimezoneOffset;
            RealDate.prototype.getTimezoneOffset = () => 0; // Mock to UTC

            const expected = '2023-10-26 10:30:00.000';
            expect(DateTime.setTimezone(utcDateTime)).toBe(expected);

            RealDate.prototype.getTimezoneOffset = originalGetTimezoneOffset; // Restore
        });

        it('should throw DatabaseException for invalid datetime input', () => {
            expect(() => DateTime.setTimezone('')).toThrow(DatabaseException);
            expect(() => DateTime.setTimezone('invalid-date-string')).toThrow(DatabaseException);
        });
    });

    describe('formatTz()', () => {
        it('should format a DB datetime string with timezone information', () => {
            const dbFormat = '2023-10-26 10:30:00.123';
            // Assuming the input dbFormat is treated as local, then converted to ISO string (UTC)
            // This behavior can be subtle. If '2023-10-26 10:30:00.123' is local,
            // its UTC representation will vary.
            // For this test, we'll construct a date assuming dbFormat is UTC for simplicity.
            const dateAsUtc = new RealDate(dbFormat + 'Z');
            const expected = dateAsUtc.toISOString();
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
