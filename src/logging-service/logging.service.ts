import { Injectable } from '@nestjs/common';
import config from '../../config/config';
import { TraceCodes } from './constants/tracecodes';

/**
 * Stand-in for flexipill-core's Winston + CloudWatch LoggingService. Same call
 * shape (traceCode first, structured meta last) so the habit transfers; the
 * transport is just stdout here.
 */
@Injectable()
export class LoggingService {
  logInfo(traceCode: TraceCodes, meta: Record<string, any> = {}): void {
    this.write('INFO', traceCode, meta);
  }

  logError(
    traceCode: TraceCodes,
    error: Error,
    meta: Record<string, any> = {},
  ): void {
    this.write('ERROR', traceCode, { ...meta, error: error?.message });
  }

  private write(
    level: string,
    traceCode: TraceCodes,
    meta: Record<string, any>,
  ): void {
    const prefix = `[${config.instanceName}] ${level} ${traceCode}`;
    console.log(prefix, JSON.stringify(meta));
  }
}
