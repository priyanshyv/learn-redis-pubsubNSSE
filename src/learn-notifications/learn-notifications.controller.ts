import { Body, Controller, Get, Header, Param, ParseIntPipe, Post, Sse } from '@nestjs/common';
import { Observable } from 'rxjs';
import { LearnNotification } from './interfaces/notification.interface';
import { SSE_DEMO_PAGE } from './constants/sse-demo-page.constant';
import { LearnNotificationsService } from './learn-notifications.service';
import { InMemoryNotificationsService } from './in-memory-notifications.service';
import { PublishNotificationDto } from './dtos/publish-notification.dto';
import { LoggingService } from '../logging-service/logging.service';
import { TraceCodes } from '../logging-service/constants/tracecodes';
import config from '../../config/config';

@Controller('learn-notifications')
export class LearnNotificationsController {
  constructor(
    private readonly learnNotificationsService: LearnNotificationsService,
    private readonly inMemoryNotificationsService: InMemoryNotificationsService,
    private readonly loggingService: LoggingService,
  ) {}

  /** Broadcast: every listener in this environment gets it. */
  @Post('publish')
  async publish(@Body() body: PublishNotificationDto) {
    try {
      const receivers = await this.learnNotificationsService.publishToAll(
        body.userId,
        body.title,
      );
      return { receivers, publishedBy: config.instanceName, env: config.env };
    } catch (err) {
      this.loggingService.logError(
        TraceCodes.LEARN_NOTIFICATION_PUBLISH_ERROR,
        err instanceof Error ? err : new Error(String(err)),
        { context: 'LearnNotificationsController.publish' },
      );
      throw err;
    }
  }

  /** Targeted: only this user's channel. Received via the pattern subscription. */
  @Post('publish-to-user')
  async publishToUser(@Body() body: PublishNotificationDto) {
    try {
      const receivers = await this.learnNotificationsService.publishToUser(
        body.userId,
        body.title,
      );
      return { receivers, publishedBy: config.instanceName, env: config.env };
    } catch (err) {
      this.loggingService.logError(
        TraceCodes.LEARN_NOTIFICATION_PUBLISH_ERROR,
        err instanceof Error ? err : new Error(String(err)),
        { context: 'LearnNotificationsController.publishToUser' },
      );
      throw err;
    }
  }

  /**
   * Server-Sent Events: a long-lived HTTP response this pod holds open and
   * writes into. @Sse turns the Observable into text/event-stream for us.
   * Each connection is an in-memory observer — NOT a new Redis connection.
   */
  @Sse('stream/:userId')
  stream(
    @Param('userId', ParseIntPipe) userId: number,
  ): Observable<{ data: LearnNotification }> {
    return this.learnNotificationsService.streamForUser(userId);
  }

  /** How many SSE connections this pod holds, vs how many Redis subs it uses. */
  @Get('stats')
  stats() {
    return {
      instance: config.instanceName,
      env: config.env,
      activeSseStreams: this.learnNotificationsService.getActiveStreamCount(),
      redisSubscriberConnections: 1,
    };
  }

  /** Browser demo page. Open http://localhost:3001/learn-notifications/demo */
  @Get('demo')
  @Header('Content-Type', 'text/html')
  demo(): string {
    return SSE_DEMO_PAGE;
  }

  /**
   * PHASE 4 — the broken pair. Same shape as the Redis endpoints above, but
   * the message never leaves this process. Publish on 3001, listen on 3002,
   * and nothing arrives. No error is raised anywhere.
   */
  @Post('in-memory/publish')
  publishInMemory(@Body() body: PublishNotificationDto) {
    const localStreams = this.inMemoryNotificationsService.publish(
      body.userId,
      body.title,
    );
    return { localStreams, publishedBy: config.instanceName };
  }

  @Sse('in-memory/stream/:userId')
  streamInMemory(
    @Param('userId', ParseIntPipe) userId: number,
  ): Observable<{ data: LearnNotification }> {
    return this.inMemoryNotificationsService.streamForUser(userId);
  }

  /** PHASE 1 teaching endpoint: returns the error text, does not throw. */
  @Post('broken-publish')
  async brokenPublish() {
    const message = await this.learnNotificationsService.publishOnSubscriber();
    return { error: message };
  }
}
