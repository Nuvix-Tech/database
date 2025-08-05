export class ID {
    /**
     * Create a new unique ID
     */
    public static unique(padding: number = 7): string {
        const timestamp = Date.now().toString(36);
        const random = Math.random().toString(36).substring(2);
        let uniqid = timestamp + random;

        if (padding > 0) {
            try {
                // Generate random bytes for additional padding
                const bytes = new Uint8Array(Math.max(1, Math.ceil(padding / 2)));
                crypto.getRandomValues(bytes);
                
                // Convert bytes to hex string
                const hex = Array.from(bytes)
                    .map(b => b.toString(16).padStart(2, '0'))
                    .join('');
                
                uniqid += hex.substring(0, padding);
            } catch (error) {
                throw new Error(`Failed to generate random bytes: ${error instanceof Error ? error.message : 'Unknown error'}`);
            }
        }

        return uniqid;
    }

    /**
     * Create a new ID from a string
     */
    public static custom(id: string): string {
        return id;
    }
}
