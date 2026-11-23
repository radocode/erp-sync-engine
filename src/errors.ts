export class TransientErpError extends Error {
  readonly retryable = true;

  constructor(message: string) {
    super(message);
    this.name = 'TransientErpError';
  }
}

export class PermanentErpError extends Error {
  readonly retryable = false;

  constructor(message: string) {
    super(message);
    this.name = 'PermanentErpError';
  }
}