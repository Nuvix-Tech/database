import { Query } from '../../src/core/query';
import { DatabaseException } from '../../src/errors/base';

describe('Query', () => {
    describe('Constructor and Getters', () => {
        it('should create a Query instance with method, attribute, and values', () => {
            const q = new Query('equal', 'age', [30]);
            expect(q.getMethod()).toBe('equal');
            expect(q.getAttribute()).toBe('age');
            expect(q.getValues()).toEqual([30]);
        });
    });

    describe('Setters', () => {
        it('should set method, attribute, values, and value', () => {
            const q = new Query('equal', 'age', [30]);
            q.setMethod('greaterThan');
            q.setAttribute('score');
            q.setValues([100]);
            q.setValue(200);
            expect(q.getMethod()).toBe('greaterThan');
            expect(q.getAttribute()).toBe('score');
            expect(q.getValues()).toEqual([200]);
        });
    });

    describe('Static isMethod', () => {
        it('should return true for supported methods', () => {
            expect(Query.isMethod('equal')).toBe(true);
            expect(Query.isMethod('limit')).toBe(true);
        });
        it('should return false for unsupported methods', () => {
            expect(Query.isMethod('unsupported')).toBe(false);
        });
    });

    describe('Static parse and parseQuery', () => {
        it('should parse a valid query string', () => {
            const q = Query.parse('{"method":"equal","attribute":"age","values":[30]}');
            expect(q).toBeInstanceOf(Query);
            expect(q.getMethod()).toBe('equal');
            expect(q.getAttribute()).toBe('age');
            expect(q.getValues()).toEqual([30]);
        });
        it('should throw DatabaseException for invalid JSON', () => {
            expect(() => Query.parse('not-a-json')).toThrow(DatabaseException);
        });
        it('should throw DatabaseException for invalid method', () => {
            expect(() => Query.parse('{"method":"invalid","attribute":"a","values":[]}')).toThrow(DatabaseException);
        });
    });

    describe('Static parseQueries', () => {
        it('should parse an array of query strings', () => {
            const queries = [
                '{"method":"equal","attribute":"age","values":[30]}',
                '{"method":"greaterThan","attribute":"score","values":[100]}'
            ];
            const result = Query.parseQueries(queries);
            expect(result.length).toBe(2);
            expect(result[0]).toBeInstanceOf(Query);
            expect(result[1]?.getMethod()).toBe('greaterThan');
        });
    });

    describe('toArray and toString', () => {
        it('should convert query to array and string', () => {
            const q = new Query('equal', 'age', [30]);
            const arr = q.toArray();
            expect(arr).toHaveProperty('method', 'equal');
            expect(arr).toHaveProperty('attribute', 'age');
            expect(arr).toHaveProperty('values');
            const str = q.toString();
            expect(typeof str).toBe('string');
            expect(JSON.parse(str)).toHaveProperty('method', 'equal');
        });
    });

    describe('Static factory methods', () => {
        it('should create queries using static methods', () => {
            expect(Query.equal('age', [30])).toBeInstanceOf(Query);
            expect(Query.notEqual('age', 25)).toBeInstanceOf(Query);
            expect(Query.lessThan('score', 100)).toBeInstanceOf(Query);
            expect(Query.greaterThan('score', 100)).toBeInstanceOf(Query);
            expect(Query.contains('tags', ['a'])).toBeInstanceOf(Query);
            expect(Query.between('score', 10, 20)).toBeInstanceOf(Query);
            expect(Query.search('name', 'john')).toBeInstanceOf(Query);
            expect(Query.select(['name', 'age'])).toBeInstanceOf(Query);
            expect(Query.orderDesc('score')).toBeInstanceOf(Query);
            expect(Query.limit(10)).toBeInstanceOf(Query);
            expect(Query.offset(5)).toBeInstanceOf(Query);
            expect(Query.isNull('deletedAt')).toBeInstanceOf(Query);
            expect(Query.or([Query.equal('a', [1]), Query.equal('b', [2])])).toBeInstanceOf(Query);
            expect(Query.and([Query.equal('a', [1]), Query.equal('b', [2])])).toBeInstanceOf(Query);
        });
    });

    describe('getByType and groupByType', () => {
        it('should filter queries by type', () => {
            const q1 = Query.equal('a', [1]);
            const q2 = Query.limit(10);
            const filtered = Query.getByType([q1, q2], ['equal']);
            expect(filtered.length).toBe(1);
            expect(filtered[0]?.getMethod()).toBe('equal');
        });
        it('should group queries by type', () => {
            const queries = [
                Query.equal('a', [1]),
                Query.limit(10),
                Query.offset(5),
                Query.orderAsc('score'),
                Query.select(['name'])
            ];
            const grouped = Query.groupByType(queries);
            expect(grouped.filters.length).toBe(1);
            expect(grouped.limit).toBe(10);
            expect(grouped.offset).toBe(5);
            expect(grouped.orderAttributes).toContain('score');
            expect(grouped.selections.length).toBe(1);
        });
    });

    describe('isNested, onArray, setOnArray, clone', () => {
        it('should identify nested queries', () => {
            const orQuery = Query.or([Query.equal('a', [1]), Query.equal('b', [2])]);
            expect(orQuery.isNested()).toBe(true);
            const eqQuery = Query.equal('a', [1]);
            expect(eqQuery.isNested()).toBe(false);
        });
        it('should set and get onArray flag', () => {
            const q = Query.equal('a', [1]);
            expect(q.onArray()).toBe(false);
            q.setOnArray(true);
            expect(q.onArray()).toBe(true);
        });
        it('should clone a query', () => {
            const q = Query.equal('a', [1]);
            const clone = q.clone();
            expect(clone).not.toBe(q);
            expect(clone.getMethod()).toBe(q.getMethod());
            expect(clone.getAttribute()).toBe(q.getAttribute());
            expect(clone.getValues()).toEqual(q.getValues());
        });
    });
});
