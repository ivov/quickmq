/** @type {import('jest').Config} */
const config = {
  preset: "ts-jest",
  testEnvironment: "node",
  testMatch: ["**/*.test.ts"],
  setupFiles: ["<rootDir>/test/jest.setup.js"],
  clearMocks: true,
  moduleNameMapper: {
    "^~/(.*)$": "<rootDir>/src/$1",
  },
  slowTestThreshold: 10,
  collectCoverageFrom: ["src/**/*.ts", "!src/logger.ts"],
};

module.exports = config;
