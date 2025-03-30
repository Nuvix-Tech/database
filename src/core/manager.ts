import { EventEmitter } from "events";

/**
 * Database Pool Manager
 *
 * A powerful pool manager that creates, caches, and manages database connection pools.
 * Optimized for handling multiple concurrent requests efficiently.
 */

// Generic pool interface that can be extended for specific database types
export interface Pool<T = any> {
    acquire(): Promise<T>;
    release(connection: T): Promise<void>;
    destroy(): Promise<void>;
    stats(): PoolStats;
}

export interface PoolStats {
    size: number;
    available: number;
    pending: number;
    max: number;
    min: number;
}

export interface PoolOptions {
    name: string;
    min?: number;
    max?: number;
    acquireTimeoutMillis?: number;
    idleTimeoutMillis?: number;
    createRetryIntervalMillis?: number;
    createTimeoutMillis?: number;
    [key: string]: any;
}

export class PoolManager extends EventEmitter {
    private static instance: PoolManager;
    private pools: Map<string, Pool>;
    private createQueue: Map<string, Promise<Pool>>;

    private constructor() {
        super();
        this.pools = new Map();
        this.createQueue = new Map();
    }

    /**
     * Get the singleton instance of PoolManager
     */
    public static getInstance(): PoolManager {
        if (!PoolManager.instance) {
            PoolManager.instance = new PoolManager();
        }
        return PoolManager.instance;
    }

    /**
     * Get or create a pool with the given name and options
     * @param name Unique identifier for the pool
     * @param createFn Function to create a new pool if it doesn't exist
     * @param options Pool configuration options
     */
    public async getPool<T>(
        name: string,
        createFn: (options: PoolOptions) => Promise<Pool<T>>,
        options: PoolOptions,
    ): Promise<Pool<T>> {
        // Check if pool exists in cache
        const existingPool = this.pools.get(name) as Pool<T>;
        if (existingPool) {
            return existingPool;
        }

        // Check if pool is currently being created
        let pendingCreate = this.createQueue.get(name);
        if (pendingCreate) {
            return pendingCreate as Promise<Pool<T>>;
        }

        // Create new pool
        const createPromise = this.createPool<T>(name, createFn, options);
        this.createQueue.set(name, createPromise);

        try {
            const pool = await createPromise;
            this.pools.set(name, pool);
            return pool;
        } finally {
            this.createQueue.delete(name);
        }
    }

    /**
     * Create a new pool
     */
    private async createPool<T>(
        name: string,
        createFn: (options: PoolOptions) => Promise<Pool<T>>,
        options: PoolOptions,
    ): Promise<Pool<T>> {
        try {
            const pool = await createFn({ name, ...(options as any) });
            this.emit("pool:created", { name, pool });
            return pool;
        } catch (error) {
            this.emit("pool:error", { name, error });
            throw error;
        }
    }

    /**
     * Release a pool by name
     */
    public async releasePool(name: string): Promise<boolean> {
        const pool = this.pools.get(name);
        if (!pool) {
            return false;
        }

        try {
            await pool.destroy();
            this.pools.delete(name);
            this.emit("pool:released", { name });
            return true;
        } catch (error) {
            this.emit("pool:error", { name, error, operation: "release" });
            throw error;
        }
    }

    /**
     * Get stats for all pools or a specific pool
     */
    public getStats(name?: string): Record<string, PoolStats> {
        if (name) {
            const pool = this.pools.get(name);
            return pool ? { [name]: pool.stats() } : {};
        }

        const stats: Record<string, PoolStats> = {};
        for (const [poolName, pool] of this.pools.entries()) {
            stats[poolName] = pool.stats();
        }
        return stats;
    }

    /**
     * Release all pools and clean up resources
     */
    public async shutdown(): Promise<void> {
        const promises: Promise<boolean>[] = [];
        for (const name of this.pools.keys()) {
            promises.push(this.releasePool(name));
        }
        await Promise.allSettled(promises);
        this.emit("shutdown");
    }
}

// Export a default instance for easy use
export default PoolManager.getInstance();
