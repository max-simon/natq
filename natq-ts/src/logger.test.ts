import { jest, describe, it, expect } from '@jest/globals';
import { DefaultLogger, LogLevel } from "./logger.js";

describe("DefaultLogger", () => {
  let consoleSpy: {
    debug: ReturnType<typeof jest.spyOn>;
    info: ReturnType<typeof jest.spyOn>;
    warn: ReturnType<typeof jest.spyOn>;
    error: ReturnType<typeof jest.spyOn>;
  };

  beforeEach(() => {
    consoleSpy = {
      debug: jest.spyOn(console, "debug").mockImplementation(() => {}),
      info: jest.spyOn(console, "info").mockImplementation(() => {}),
      warn: jest.spyOn(console, "warn").mockImplementation(() => {}),
      error: jest.spyOn(console, "error").mockImplementation(() => {}),
    };
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  describe("with DEBUG level", () => {
    it("should log debug messages", () => {
      const logger = new DefaultLogger(LogLevel.DEBUG);
      logger.debug("test message", { data: 1 });
      expect(consoleSpy.debug).toHaveBeenCalledWith("test message", { data: 1 });
    });

    it("should log info messages", () => {
      const logger = new DefaultLogger(LogLevel.DEBUG);
      logger.info("test message");
      expect(consoleSpy.info).toHaveBeenCalledWith("test message");
    });

    it("should log warn messages", () => {
      const logger = new DefaultLogger(LogLevel.DEBUG);
      logger.warn("test message");
      expect(consoleSpy.warn).toHaveBeenCalledWith("test message");
    });

    it("should log error messages", () => {
      const logger = new DefaultLogger(LogLevel.DEBUG);
      logger.error("test message");
      expect(consoleSpy.error).toHaveBeenCalledWith("test message");
    });
  });

  describe("with INFO level", () => {
    it("should not log debug messages", () => {
      const logger = new DefaultLogger(LogLevel.INFO);
      logger.debug("test message");
      expect(consoleSpy.debug).not.toHaveBeenCalled();
    });

    it("should log info messages", () => {
      const logger = new DefaultLogger(LogLevel.INFO);
      logger.info("test message");
      expect(consoleSpy.info).toHaveBeenCalledWith("test message");
    });

    it("should log warn messages", () => {
      const logger = new DefaultLogger(LogLevel.INFO);
      logger.warn("test message");
      expect(consoleSpy.warn).toHaveBeenCalledWith("test message");
    });

    it("should log error messages", () => {
      const logger = new DefaultLogger(LogLevel.INFO);
      logger.error("test message");
      expect(consoleSpy.error).toHaveBeenCalledWith("test message");
    });
  });

  describe("with WARN level", () => {
    it("should not log debug messages", () => {
      const logger = new DefaultLogger(LogLevel.WARN);
      logger.debug("test message");
      expect(consoleSpy.debug).not.toHaveBeenCalled();
    });

    it("should not log info messages", () => {
      const logger = new DefaultLogger(LogLevel.WARN);
      logger.info("test message");
      expect(consoleSpy.info).not.toHaveBeenCalled();
    });

    it("should log warn messages", () => {
      const logger = new DefaultLogger(LogLevel.WARN);
      logger.warn("test message");
      expect(consoleSpy.warn).toHaveBeenCalledWith("test message");
    });

    it("should log error messages", () => {
      const logger = new DefaultLogger(LogLevel.WARN);
      logger.error("test message");
      expect(consoleSpy.error).toHaveBeenCalledWith("test message");
    });
  });

  describe("with ERROR level", () => {
    it("should not log debug messages", () => {
      const logger = new DefaultLogger(LogLevel.ERROR);
      logger.debug("test message");
      expect(consoleSpy.debug).not.toHaveBeenCalled();
    });

    it("should not log info messages", () => {
      const logger = new DefaultLogger(LogLevel.ERROR);
      logger.info("test message");
      expect(consoleSpy.info).not.toHaveBeenCalled();
    });

    it("should not log warn messages", () => {
      const logger = new DefaultLogger(LogLevel.ERROR);
      logger.warn("test message");
      expect(consoleSpy.warn).not.toHaveBeenCalled();
    });

    it("should log error messages", () => {
      const logger = new DefaultLogger(LogLevel.ERROR);
      logger.error("test message");
      expect(consoleSpy.error).toHaveBeenCalledWith("test message");
    });
  });

  describe("default level", () => {
    it("should default to INFO level", () => {
      const logger = new DefaultLogger();
      logger.debug("test message");
      logger.info("test message");
      expect(consoleSpy.debug).not.toHaveBeenCalled();
      expect(consoleSpy.info).toHaveBeenCalled();
    });
  });
});
