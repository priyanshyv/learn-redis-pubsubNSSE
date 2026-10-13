import { Injectable } from '@nestjs/common';
import { Observable, Subject, filter, map } from 'rxjs';
import config from '../../config/config';
import { LoggingService } from '../logging-service/logging.service';
import { TraceCodes } from '../logging-service/constants/tracecodes';
import { LearnNotification } from './interfaces/notification.interface';

/**
 * PHASE 4 — the WRONG implementation, kept on purpose.
 *
 * Identical to LearnNotificationsService from the SSE layer down: same Subject,
 * same filter, same stream shape. The only difference is that publish() calls
 * local$.next() directly instead of going through Redis.
 *
 * On one process this is indistinguishable from the correct version — every
 * test passes, the demo page works. It breaks the moment a second process
 * exists, and it breaks silently: no error, no log, the publisher still gets a
 * 200 back. This is what an in-memory EventEmitter buys you.
 */
@Injectable()
export class InMemoryNotificationsService {
  private readonly local$ = new Subject<LearnNotification>();
  private activeStreams = 0;

  constructor(private readonly loggingService: LoggingService) {}

  /** Never leaves this process. No network hop, nothing to cross a pod boundary. */
  publish(userId: number, title: string): number {
    const notification: LearnNotification = {
      userId,
      title,
      publishedAt: Date.now(),
      publishedBy: config.instanceName,
    };

    this.local$.next(notification);

    this.loggingService.logInfo(
      TraceCodes.LEARN_NOTIFICATION_IN_MEMORY_PUBLISHED,
      { userId, instance: config.instanceName, activeStreams: this.activeStreams },
    );

    // There is no receiver count to report. Nothing counted anything.
    return this.activeStreams;
  }

  streamForUser(userId: number): Observable<{ data: LearnNotification }> {
    return new Observable<{ data: LearnNotification }>((observer) => {
      this.activeStreams += 1;

      const sub = this.local$
        .pipe(
          filter((n) => n.userId === userId),
          map((n) => ({ data: n })),
        )
        .subscribe(observer);

      return () => {
        sub.unsubscribe();
        this.activeStreams -= 1;
      };
    });
  }

  getActiveStreamCount(): number {
    return this.activeStreams;
  }
}
