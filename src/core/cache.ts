import { Cache as NuvixCache } from '@nuvix/cache';
import { Base } from './base.js';

export class Cache extends Base {
    protected cacheName: string = 'default';

    public getCache(): NuvixCache {
        return this.cache;
    }


}
