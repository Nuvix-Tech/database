import { Database } from "../src/core/database";
import { Document } from "../src/core/Document";
import { Query } from "../src/core/query";
import { Adapter } from "../src/adapter/base";
import { PostgreDB } from "../src/adapter/postgre";
import { DB } from "./config";
import { Cache, RedisAdapter } from "@nuvix/cache";
import Permission from "../src/security/Permission";
import Role from "../src/security/Role";
import { Authorization } from "../src/security/authorization";

const APP_LIMIT_SUBSCRIBERS_SUBQUERY = 1000;
const APP_LIMIT_SUBQUERY = 1000;

export const filters = {
    casting: {
        serialize: (value: any) => {
            return JSON.stringify({ value: value }, (key, value) => {
                return typeof value === "number" && !isFinite(value)
                    ? String(value)
                    : value;
            });
        },
        deserialize: (value: any) => {
            if (value == null || value === undefined) {
                return null;
            }

            return JSON.parse(value)?.value;
        },
    },
    enum: {
        serialize: ((value: any, attribute: Document) => {
            if (attribute.isSet("elements")) {
                attribute.removeAttribute("elements");
            }

            return value;
        }) as any,
        deserialize: ((value: any, attribute: Document) => {
            const formatOptions = JSON.parse(
                attribute.getAttribute("formatOptions", "[]"),
            );
            if (formatOptions.elements) {
                attribute.setAttribute("elements", formatOptions.elements);
            }

            return value;
        }) as any,
    },
    range: {
        serialize: (value: any, attribute: Document) => {
            if (attribute.isSet("min")) {
                attribute.removeAttribute("min");
            }
            if (attribute.isSet("max")) {
                attribute.removeAttribute("max");
            }

            return value;
        },
        deserialize: (value: any, attribute: Document) => {
            const formatOptions = JSON.parse(
                attribute.getAttribute("formatOptions", "[]"),
            );
            if (formatOptions.min || formatOptions.max) {
                attribute
                    .setAttribute("min", formatOptions.min)
                    .setAttribute("max", formatOptions.max);
            }

            return value;
        },
    },
    subQueryAttributes: {
        serialize: (value: any) => {
            return null;
        },
        deserialize: async (
            value: any,
            document: Document,
            database: Database,
        ) => {
            const attributes = await database.find("attributes", [
                Query.equal("collectionInternalId", [document.getInternalId()]),
                Query.limit(database.getLimitForAttributes()),
            ]);

            attributes.forEach((attribute) => {
                if (
                    attribute.getAttribute("type") === Database.VAR_RELATIONSHIP
                ) {
                    const options = attribute.getAttribute("options");
                    Object.keys(options).forEach((key) => {
                        attribute.setAttribute(key, options[key]);
                    });
                    attribute.removeAttribute("options");
                }
            });

            return attributes;
        },
    },
    subQueryIndexes: {
        serialize: (value: any) => {
            return null;
        },
        deserialize: async (
            value: any,
            document: Document,
            database: Database,
        ) => {
            return await database.find("indexes", [
                Query.equal("collectionInternalId", [document.getInternalId()]),
                Query.limit(database.getLimitForIndexes()),
            ]);
        },
    },
    subQueryPlatforms: {
        serialize: (value: any) => {
            return null;
        },
        deserialize: async (
            value: any,
            document: Document,
            database: Database,
        ) => {
            return await database.find("platforms", [
                Query.equal("projectInternalId", [document.getInternalId()]),
                Query.limit(APP_LIMIT_SUBQUERY),
            ]);
        },
    },

    subQueryKeys: {
        serialize: (value: any) => {
            return null;
        },
        deserialize: async (
            value: any,
            document: Document,
            database: Database,
        ) => {
            return await database.find("keys", [
                Query.equal("projectInternalId", [document.getInternalId()]),
                Query.limit(APP_LIMIT_SUBQUERY),
            ]);
        },
    },
    subQueryWebhooks: {
        serialize: (value: any) => {
            return null;
        },
        deserialize: async (
            value: any,
            document: Document,
            database: Database,
        ) => {
            return await database.find("webhooks", [
                Query.equal("projectInternalId", [document.getInternalId()]),
                Query.limit(APP_LIMIT_SUBQUERY),
            ]);
        },
    },
    subQuerySessions: {
        serialize: (value: any) => {
            return null;
        },
        deserialize: async (
            value: any,
            document: Document,
            database: Database,
        ) => {
            return await Authorization.skip(async () => {
                return await database.find("sessions", [
                    Query.equal("userInternalId", [document.getInternalId()]),
                    Query.limit(APP_LIMIT_SUBQUERY),
                ]);
            });
        },
    },
    subQueryTokens: {
        serialize: (value: any) => {
            return null;
        },
        deserialize: async (
            value: any,
            document: Document,
            database: Database,
        ) => {
            return await Authorization.skip(async () => {
                return await database.find("tokens", [
                    Query.equal("userInternalId", [document.getInternalId()]),
                    Query.limit(APP_LIMIT_SUBQUERY),
                ]);
            });
        },
    },
    subQueryChallenges: {
        serialize: (value: any) => {
            return null;
        },
        deserialize: async (
            value: any,
            document: Document,
            database: Database,
        ) => {
            return await Authorization.skip(async () => {
                return await database.find("challenges", [
                    Query.equal("userInternalId", [document.getInternalId()]),
                    Query.limit(APP_LIMIT_SUBQUERY),
                ]);
            });
        },
    },
    subQueryAuthenticators: {
        serialize: (value: any) => {
            return null;
        },
        deserialize: async (
            value: any,
            document: Document,
            database: Database,
        ) => {
            return await Authorization.skip(async () => {
                return await database.find("authenticators", [
                    Query.equal("userInternalId", [document.getInternalId()]),
                    Query.limit(APP_LIMIT_SUBSCRIBERS_SUBQUERY),
                ]);
            });
        },
    },
    subQueryMemberships: {
        serialize: (value: any) => {
            return null;
        },
        deserialize: async (
            value: any,
            document: Document,
            database: Database,
        ) => {
            return await Authorization.skip(async () => {
                return await database.find("memberships", [
                    Query.equal("userInternalId", [document.getInternalId()]),
                    Query.limit(APP_LIMIT_SUBSCRIBERS_SUBQUERY),
                ]);
            });
        },
    },
    subQueryVariables: {
        serialize: (value: any) => {
            return null;
        },
        deserialize: async (
            value: any,
            document: Document,
            database: Database,
        ) => {
            return await database.find("variables", [
                Query.equal("resourceInternalId", [document.getInternalId()]),
                Query.equal("resourceType", ["function"]),
                Query.limit(APP_LIMIT_SUBSCRIBERS_SUBQUERY),
            ]);
        },
    },

    subQueryProjectVariables: {
        serialize: (value: any) => {
            return null;
        },
        deserialize: async (
            value: any,
            document: Document,
            database: Database,
        ) => {
            return await database.find("variables", [
                Query.equal("resourceType", ["project"]),
                Query.limit(APP_LIMIT_SUBSCRIBERS_SUBQUERY),
            ]);
        },
    },
    userSearch: {
        serialize: (value: any, user: Document) => {
            const searchValues = [
                user.getId(),
                user.getAttribute("email", ""),
                user.getAttribute("name", ""),
                user.getAttribute("phone", ""),
            ];

            user.getAttribute("labels", []).forEach((label: string) => {
                searchValues.push("label:" + label);
            });

            return searchValues.filter(Boolean).join(" ");
        },
        deserialize: (value: any) => {
            return value;
        },
    },
    subQueryTargets: {
        serialize: (value: any) => {
            return null;
        },
        deserialize: async (
            value: any,
            document: Document,
            database: Database,
        ) => {
            return await Authorization.skip(async () => {
                return await database.find("targets", [
                    Query.equal("userInternalId", [document.getInternalId()]),
                    Query.limit(APP_LIMIT_SUBSCRIBERS_SUBQUERY),
                ]);
            });
        },
    },
    // subProjectQueryTargets: {
    //   serialize: (value: any) => {
    //     return null;
    //   },
    //   deserialize: async (value: any, document: Document, database: Database) => {
    //     return await Authorization.skip(async () => {
    //       const db = new Database(database.getAdapter(), database.getCache());
    //       db.setDatabase('messaging');
    //       return await db.find('targets', [
    //         Query.equal('userInternalId', [document.getInternalId()]),
    //         Query.limit(APP_LIMIT_SUBQUERY),
    //       ]);
    //     });
    //   },
    // },
    subQueryTopicTargets: {
        serialize: (value: any) => {
            return null;
        },
        deserialize: async (
            value: any,
            document: Document,
            database: Database,
        ) => {
            const targetIds = await Authorization.skip(async () => {
                const subscribers = await database.find("subscribers", [
                    Query.equal("topicInternalId", [document.getInternalId()]),
                    Query.limit(APP_LIMIT_SUBSCRIBERS_SUBQUERY),
                ]);
                return subscribers.map((subscriber: Document) =>
                    subscriber.getAttribute("targetInternalId"),
                );
            });

            if (targetIds.length > 0) {
                return await database.skipValidation(async () => {
                    return await database.find("targets", [
                        Query.equal("$internalId", targetIds),
                    ]);
                });
            }
            return [];
        },
    },
    providerSearch: {
        serialize: (value: any, provider: Document) => {
            const searchValues = [
                provider.getId(),
                provider.getAttribute("name", ""),
                provider.getAttribute("provider", ""),
                provider.getAttribute("type", ""),
            ];

            return searchValues.filter(Boolean).join(" ");
        },
        deserialize: (value: any) => {
            return value;
        },
    },
    topicSearch: {
        serialize: (value: any, topic: Document) => {
            const searchValues = [
                topic.getId(),
                topic.getAttribute("name", ""),
                topic.getAttribute("description", ""),
            ];

            return searchValues.filter(Boolean).join(" ");
        },
        deserialize: (value: any) => {
            return value;
        },
    },
    messageSearch: {
        serialize: (value: any, message: Document) => {
            const searchValues = [
                message.getId(),
                message.getAttribute("description", ""),
                message.getAttribute("status", ""),
            ];

            const data = JSON.parse(message.getAttribute("data", "{}"));
            const providerType = message.getAttribute("providerType", "");

            if (providerType === "email") {
                searchValues.push(data.subject, "email");
            } else if (providerType === "sms") {
                searchValues.push(data.content, "sms");
            } else {
                searchValues.push(data.title, "push");
            }

            return searchValues.filter(Boolean).join(" ");
        },
        deserialize: (value: any) => {
            return value;
        },
    },
};

// export const formats = {
//   [APP_DATABASE_ATTRIBUTE_EMAIL]: {
//     create: () => new EmailValidator(),
//     type: Database.VAR_STRING,
//   },
//   [APP_DATABASE_ATTRIBUTE_DATETIME]: {
//     create: () => new DatetimeValidator(),
//     type: Database.VAR_DATETIME,
//   },
//   [APP_DATABASE_ATTRIBUTE_ENUM]: {
//     create: (attribute: any) => {
//       const elements = attribute.formatOptions.elements;
//       return new WhiteList(elements, true);
//     },
//     type: Database.VAR_STRING,
//   },
//   [APP_DATABASE_ATTRIBUTE_IP]: {
//     create: () => new IPValidator(),
//     type: Database.VAR_STRING,
//   },
//   [APP_DATABASE_ATTRIBUTE_URL]: {
//     create: () => new URLValidator(),
//     type: Database.VAR_STRING,
//   },
//   [APP_DATABASE_ATTRIBUTE_INT_RANGE]: {
//     create: (attribute: any) => {
//       const min = attribute.formatOptions.min ?? -Infinity;
//       const max = attribute.formatOptions.max ?? Infinity;
//       return new RangeValidator(min, max, `integer`);
//     },
//     type: Database.VAR_INTEGER,
//   },
//   [APP_DATABASE_ATTRIBUTE_FLOAT_RANGE]: {
//     create: (attribute: any) => {
//       const min = attribute.formatOptions.min ?? -Infinity;
//       const max = attribute.formatOptions.max ?? Infinity;
//       return new RangeValidator(min, max, `float`);
//     },
//     type: Database.VAR_FLOAT,
//   },
// };

Object.keys(filters).forEach((key) => {
    const filterKey = key as keyof typeof filters;
    Database.addFilter(key, {
        encode: filters[filterKey].serialize,
        decode: filters[filterKey].deserialize,
    });
});

// Object.keys(formats).forEach(key => {
//   Structure.addFormat(key, formats[key].create, formats[key].type);
// });

/**
 * Gets an initialized database adapter for testing
 * This factory allows the tests to work with any adapter implementation
 */
function getAdapter(): Adapter {
    const ssl = false;
    // Create a PostgreSQL adapter by default
    // In a production environment, you would inject the adapter based on configuration
    const adapter = new PostgreDB({
        connection: {
            connectionString:
                "postgres://nuvix_admin:testpassword@35.244.24.126:6432/postgres",
            ssl: ssl
                ? {
                      rejectUnauthorized: false,
                  }
                : undefined,
        },
        schema: "messaging",
    });

    adapter.init();
    return adapter;
}

// Skip tests if adapter connection isn't possible
const runTests = process.env["SKIP_DB_TESTS"] !== "true";

describe("Database Core", () => {
    let adapter: Adapter;
    let db: Database;
    let cache: Cache;

    // Set higher timeout for tests
    jest.setTimeout(60000);

    beforeAll(async () => {
        if (!runTests) {
            console.log(
                "Skipping database tests. Set SKIP_DB_TESTS=false to run.",
            );
            return;
        }

        try {
            // Initialize adapter
            adapter = getAdapter();
            await (adapter as PostgreDB).ping();

            cache = new Cache(
                new RedisAdapter({
                    host: "localhost",
                    port: 6379,
                    namespace: "test-core",
                }),
            );
            db = new Database(adapter, cache, {
                logger: true,
            });
        } catch (err) {
            console.error("Error setting up database test:", err);
            throw err;
        }
    });

    describe("Collection Operations", () => {
        Authorization.disable();
        test("should get a collection", async () => {
            if (!runTests) return;

            const collections = await db.count("messages", [
                Query.search("search", "684049870019118ae992"),
                Query.limit(10),
                Query.offset(0),
            ]);
            console.log("Collection:", collections);
            expect(collections).toBeTruthy();
        });
    });
});
