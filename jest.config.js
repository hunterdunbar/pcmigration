module.exports = {
    testEnvironment: 'node',
    coverageDirectory: 'coverage',
    collectCoverageFrom: [
        'services/**/*.js',
        'controllers/**/*.js',
        '!**/node_modules/**'
    ],
    testMatch: [
        '**/tests/**/*.test.js'
    ],
    verbose: true,
    testTimeout: 10000
};

