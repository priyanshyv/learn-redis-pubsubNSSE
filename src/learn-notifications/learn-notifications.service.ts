import { Injectable, OnModuleInit, OnModuleDestroy } from '@nestjs/common';
import Redis from 'ioredis';
import { Observable, Subject, filter, map } from 'rxjs';
import config from '../../config/config';
import { LoggingService } from '../logging-service/logging.service';
import { TraceCodes } from '../logging-service/constants/tracecodes';
import { isShuttingDown } from '../shutdown.state';
import { LearnNotification } from './interfaces/notification.interface';
import { LEARN_NOTIFICATION_CHANNELS } from './constants/learn-notifications.constants';

@Injectable()
export class LearnNotificationsService
  implements OnModuleInit, OnModuleDestroy
{
  // Two connections, not one. A connection that has issued SUBSCRIBE is in
  // "subscriber mode" and the server will only accept subscribe-family commands
  // on it; ioredis enforces the same rule client-side. See publishOnSubscriber().
  private subscriber: Redis | null = null;
  private publisher: Redis | null = null;

  // In-process fan-out. ONE Redis subscription feeds this Subject; every open
  // SSE connection on this pod is an in-memory observer of it. Without this,
  // each connected browser would need its own Redis connection — see PHASE 3.
  private readonly local$ = new Subject<LearnNotification>();

  // Observability only: how many SSE connections this pod is holding.
  private activeStreams = 0;

  constructor(private readonly loggingService: LoggingService) {}

  private buildRedis(role: string, errorCode: TraceCodes): Redis {
    const client = new Redis({
      host: config.redisConfig.host,
      port: config.redisConfig.port,
    });

    // An 'error' listener is mandatory: an ioredis client with no error handler
    // emits an unhandled 'error' event, which takes the whole process down.
    // Publisher and subscriber get distinct codes — "Redis is broken" is not an
    // actionable alert, "we are publishing into the void" is.
    client.on('error', (err: Error) => {
      this.loggingService.logError(errorCode, err, {
        context: `LearnNotificationsService.${role}`,
      });
    });

    return client;
  }

  onModuleInit(): void {
    try {
      this.publisher = this.buildRedis(
        'publisher',
        TraceCodes.LEARN_NOTIFICATION_PUBLISHER_ERROR,
      );
      this.subscriber = this.buildRedis(
        'subscriber',
        TraceCodes.LEARN_NOTIFICATION_SUBSCRIBER_ERROR,
      );

      // Register handlers before subscribing so no message can land in the gap
      // between SUBSCRIBE completing and the listener being attached.
      // Exact-match and pattern deliveries arrive as DIFFERENT events, and a
      // pattern delivery carries the pattern as well as the concrete channel.
      this.subscriber.on('message', (channel: string, raw: string) => {
        this.handleMessage(channel, raw);
      });
      this.subscriber.on(
        'pmessage',
        (pattern: string, channel: string, raw: string) => {
          this.handlePatternMessage(pattern, channel, raw);
        },
      );

      void this.subscriber.subscribe(LEARN_NOTIFICATION_CHANNELS.ALL);
      void this.subscriber.psubscribe(
        LEARN_NOTIFICATION_CHANNELS.USER_PATTERN,
      );

      this.loggingService.logInfo(TraceCodes.LEARN_NOTIFICATION_PUBSUB_INIT, {
        env: config.env,
        channel: LEARN_NOTIFICATION_CHANNELS.ALL,
        pattern: LEARN_NOTIFICATION_CHANNELS.USER_PATTERN,
        instance: config.instanceName,
      });
    } catch (err) {
      // Pub/sub is a best-effort push path. Failing to wire it must not stop
      // the process from booting and serving everything else.
      this.loggingService.logError(
        TraceCodes.LEARN_NOTIFICATION_PUBSUB_INIT_ERROR,
        err instanceof Error ? err : new Error(String(err)),
        { context: 'LearnNotificationsService.onModuleInit' },
      );
    }
  }

  async onModuleDestroy(): Promise<void> {
    this.loggingService.logInfo(TraceCodes.LEARN_NOTIFICATION_SHUTDOWN, {
      instance: config.instanceName,
    });
    try {
      // quit() sends QUIT and drains, unlike disconnect() which drops the socket.
      if (this.subscriber) await this.subscriber.quit();
      if (this.publisher) await this.publisher.quit();
    } catch {
      // Best-effort on shutdown: the process is going away regardless.
    }
  }

  private handleMessage(channel: string, raw: string): void {
    if (!this.shouldProcess(channel)) return;

    const notification = this.parseNotification(raw);
    if (!notification) return;

    this.loggingService.logInfo(TraceCodes.LEARN_NOTIFICATION_RECEIVED, {
      channel,
      userId: notification.userId,
      title: notification.title,
      publishedBy: notification.publishedBy,
      lagMs: Date.now() - notification.publishedAt,
      activeStreams: this.activeStreams,
    });

    this.local$.next(notification);
  }

  private handlePatternMessage(
    pattern: string,
    channel: string,
    raw: string,
  ): void {
    if (!this.shouldProcess(channel)) return;

    const notification = this.parseNotification(raw);
    if (!notification) return;

    this.loggingService.logInfo(
      TraceCodes.LEARN_NOTIFICATION_PATTERN_RECEIVED,
      {
        pattern,
        channel,
        userId: notification.userId,
        title: notification.title,
        publishedBy: notification.publishedBy,
      },
    );

    this.local$.next(notification);
  }

  /**
   * Mid-shutdown this pod still holds a live subscription until quit() lands.
   * Starting new work now risks being killed halfway through it, so drop the
   * message rather than half-process it. At-most-once already permits a drop.
   */
  private shouldProcess(channel: string): boolean {
    if (isShuttingDown) {
      this.loggingService.logInfo(
        TraceCodes.LEARN_NOTIFICATION_RECEIVE_SKIPPED_SHUTDOWN,
        { channel },
      );
      return false;
    }
    return true;
  }

  /** Guard at the boundary: this JSON came off the network, trust nothing. */
  private parseNotification(raw: string): LearnNotification | null {
    try {
      const parsed = JSON.parse(raw);
      if (!parsed || typeof parsed.userId !== 'number') {
        this.loggingService.logInfo(
          TraceCodes.LEARN_NOTIFICATION_MALFORMED_MESSAGE,
          { raw },
        );
        return null;
      }
      return parsed as LearnNotification;
    } catch (err) {
      this.loggingService.logError(
        TraceCodes.LEARN_NOTIFICATION_MALFORMED_MESSAGE,
        err instanceof Error ? err : new Error(String(err)),
        { raw },
      );
      return null;
    }
  }

  /** Broadcast to everyone listening in this environment. */
  async publishToAll(userId: number, title: string): Promise<number> {
    return this.publishTo(LEARN_NOTIFICATION_CHANNELS.ALL, userId, title);
  }

  /** Targeted send. Only subscribers of this one user's channel receive it. */
  async publishToUser(userId: number, title: string): Promise<number> {
    return this.publishTo(
      LEARN_NOTIFICATION_CHANNELS.USER(userId),
      userId,
      title,
    );
  }

  private async publishTo(
    channel: string,
    userId: number,
    title: string,
  ): Promise<number> {
    if (!this.publisher) return 0;

    if (isShuttingDown) {
      this.loggingService.logInfo(
        TraceCodes.LEARN_NOTIFICATION_PUBLISH_SKIPPED_SHUTDOWN,
        { userId, channel },
      );
      return 0;
    }

    const notification: LearnNotification = {
      userId,
      title,
      publishedAt: Date.now(),
      publishedBy: config.instanceName,
    };

    try {
      // The return value is how many subscriber connections Redis wrote to.
      // It is a routing receipt, not an acknowledgement that anyone acted on it.
      const receivers = await this.publisher.publish(
        channel,
        JSON.stringify(notification),
      );

      this.loggingService.logInfo(TraceCodes.LEARN_NOTIFICATION_PUBLISHED, {
        userId,
        channel,
        receivers,
      });

      return receivers;
    } catch (err) {
      this.loggingService.logError(
        TraceCodes.LEARN_NOTIFICATION_PUBLISH_ERROR,
        err instanceof Error ? err : new Error(String(err)),
        { context: 'LearnNotificationsService.publishTo', userId, channel },
      );
      return 0;
    }
  }

  /**
   * One Observable per connected browser, filtered to that user. These are
   * in-memory subscriptions to a Subject — they cost a closure, not a socket.
   * The Redis subscription count does not change when a client connects.
   */
  streamForUser(userId: number): Observable<{ data: LearnNotification }> {
    return new Observable<{ data: LearnNotification }>((observer) => {
      // Counted HERE, not in the enclosing method. The Observable body runs
      // once per subscribe; the method body runs once per call. Those are not
      // the same number, and the counter must match the teardown below.
      this.activeStreams += 1;
      this.loggingService.logInfo(TraceCodes.LEARN_NOTIFICATION_STREAM_OPENED, {
        userId,
        activeStreams: this.activeStreams,
      });

      const sub = this.local$
        .pipe(
          filter((n) => n.userId === userId),
          map((n) => ({ data: n })),
        )
        .subscribe(observer);

      // Runs when the browser disconnects. Without this the Subject keeps a
      // reference to a dead subscriber forever — a textbook leak.
      return () => {
        sub.unsubscribe();
        this.activeStreams -= 1;
        this.loggingService.logInfo(
          TraceCodes.LEARN_NOTIFICATION_STREAM_CLOSED,
          { userId, activeStreams: this.activeStreams },
        );
      };
    });
  }

  getActiveStreamCount(): number {
    return this.activeStreams;
  }

  /**
   * Deliberately wrong, kept from PHASE 1. Publishes on the connection that is
   * already in subscriber mode, to show the exact error you get when you try to
   * save a connection by reusing one client for both directions.
   */
  async publishOnSubscriber(): Promise<string> {
    try {
      await this.subscriber.publish(
        LEARN_NOTIFICATION_CHANNELS.ALL,
        'this will not work',
      );
      return 'unexpected: the publish succeeded';
    } catch (err) {
      return err instanceof Error ? err.message : String(err);
    }
  }
}
