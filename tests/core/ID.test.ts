import { ID } from '../../src/core/ID';

describe('ID', () => {
    describe('custom()', () => {
        it('should return the custom ID', () => {
            const customId = 'my-custom-id';
            expect(ID.custom(customId)).toBe(customId);
        });

        it('should return an empty string if an empty string is provided', () => {
            expect(ID.custom('')).toBe('');
        });
    });

    describe('unique()', () => {
        it('should generate a unique ID', () => {
            const id1 = ID.unique();
            const id2 = ID.unique();
            expect(id1).toBeDefined();
            expect(id2).toBeDefined();
            expect(id1).not.toBe(id2);
        });

        it('should generate an ID of the correct length with default padding', () => {
            const id = ID.unique();
            // Default padding is 7. Timestamp hex length varies, but is usually around 13-14.
            // So, total length should be around 20-21.
            // This is a somewhat loose check due to timestamp variability.
            expect(id.length).toBeGreaterThanOrEqual(18); // Adjusted for potential shorter timestamps
            expect(id.length).toBeLessThanOrEqual(25); // Adjusted for potential longer timestamps
        });

        it('should generate an ID of the correct length with custom padding', () => {
            const padding = 10;
            const id = ID.unique(padding);
            // Timestamp hex length + custom padding
            // Similar to default padding, this is a somewhat loose check.
            expect(id.length).toBeGreaterThanOrEqual(18 + (padding - 7));
            expect(id.length).toBeLessThanOrEqual(25 + (padding - 7));
        });

        it('should generate a unique ID with zero padding', () => {
            const id = ID.unique(0);
            expect(id).toBeDefined();
            // Length should be only the hex timestamp part
            expect(id.length).toBeGreaterThanOrEqual(11);
            expect(id.length).toBeLessThanOrEqual(18);
        });

        it('should generate different IDs in quick succession', () => {
            const ids = new Set();
            for (let i = 0; i < 100; i++) {
                ids.add(ID.unique());
            }
            expect(ids.size).toBe(100);
        });
    });
});
