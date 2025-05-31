export const testEnvironment = "node";
export const transform = {
    "^.+\\.ts$": "ts-jest",
};
export const testRegex = "(/__tests__/.*|(\\.|/))(test|spec)\\.(ts|js|mjs)$";
export const moduleFileExtensions = ["ts", "js", "mjs", "json", "node"];
