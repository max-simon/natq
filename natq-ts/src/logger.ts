/**
 * Task logger interface
 */
export interface Logger {
  debug(...args: any[]): void;
  info(...args: any[]): void;
  warn(...args: any[]): void;
  error(...args: any[]): void;
}

export enum LogLevel {
    DEBUG = "debug",
    INFO = "info",
    WARN = "warn",
    ERROR = "error",
}

const LOG_LEVEL_PRIORITY: Record<LogLevel, number> = {
    [LogLevel.DEBUG]: 0,
    [LogLevel.INFO]: 1,
    [LogLevel.WARN]: 2,
    [LogLevel.ERROR]: 3,
};

export class DefaultLogger implements Logger {
    private levelPriority: number;

    constructor(level: LogLevel = LogLevel.INFO) {
        this.levelPriority = LOG_LEVEL_PRIORITY[level];
    }

    debug(...args: any[]) {
        if (this.levelPriority <= LOG_LEVEL_PRIORITY[LogLevel.DEBUG]) {
            console.debug(...args);
        }
    }

    info(...args: any[]) {
        if (this.levelPriority <= LOG_LEVEL_PRIORITY[LogLevel.INFO]) {
            console.info(...args);
        }
    }

    warn(...args: any[]) {
        if (this.levelPriority <= LOG_LEVEL_PRIORITY[LogLevel.WARN]) {
            console.warn(...args);
        }
    }

    error(...args: any[]) {
        if (this.levelPriority <= LOG_LEVEL_PRIORITY[LogLevel.ERROR]) {
            console.error(...args);
        }
    }
}
