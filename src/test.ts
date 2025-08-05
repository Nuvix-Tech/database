import { Adapter } from "@adapters/adapter.js";
import { Database } from "@core/database.js";
import { Doc } from "@core/doc.js";
import { AttributeEnum, EventsEnum, IndexEnum } from "@core/enums.js";
import { Cache, Memory } from "@nuvix/cache";
import { Permission } from "@utils/permission.js";
import { Role } from "@utils/role.js";
import { Attribute } from "@validators/schema.js";
import { Pool } from "pg";

export const DB = `postgres://ravikant:Ravi%40saini12@localhost:5432/yyy`

const adapter = new Adapter({
    connectionString: DB,
    max: 10, // Maximum number of clients in the pool
});
adapter.setMeta({
    database: 'yyy',
    namespace: 'r1',
    schema: 'test3',
})
const cache = new Cache(new Memory());

const db = new Database(adapter, cache)

// try {
//     await db.create();
// } catch (error) {
//     console.error('Error creating database:', error);
// }

const attrs = new Doc<{ attributes: Attribute[] }>({
    attributes: [
        {
            $id: 'name',
            key: 'name',
            type: AttributeEnum.String,
            size: 100,
            required: true,
        },
        {
            $id: 'email',
            key: 'email',
            type: AttributeEnum.String,
            required: true,
            size: 100,
        },
        {
            $id: 'age',
            key: 'age',
            type: AttributeEnum.Integer,
            required: false,
            default: 7,
        }
    ] as any
})

adapter.before(EventsEnum.All, 'HELLO', (sql) => {
    console.log('Before event:', sql);
    return sql;
});

db.on(EventsEnum.All, 'HELLO4', (...args: any[]) => {
    console.log('Event triggered:', args);
});

try {
    const coll = await db.createCollection({
        id: 'users',
        attributes: attrs.get('attributes'),
        indexes: [
            new Doc({
                $id: 'email_index',
                type: IndexEnum.Unique,
                attributes: ['email'],
            })
        ],
    });
    console.log('Collection created:', coll.getId(), coll.getSequence());
} catch (error) {
    console.error('Error creating collection:', error);
}
