import { Module } from '@nestjs/common';
import { LearnNotificationsController } from './learn-notifications.controller';
import { LearnNotificationsService } from './learn-notifications.service';
import { InMemoryNotificationsService } from './in-memory-notifications.service';

@Module({
  controllers: [LearnNotificationsController],
  providers: [LearnNotificationsService, InMemoryNotificationsService],
  exports: [LearnNotificationsService, InMemoryNotificationsService],
})
export class LearnNotificationsModule {}
