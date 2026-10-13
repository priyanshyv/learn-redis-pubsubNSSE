import { Module } from '@nestjs/common';
import { LoggingModule } from './logging-service/logging.module';
import { LearnNotificationsModule } from './learn-notifications/learn-notifications.module';

@Module({
  imports: [LoggingModule, LearnNotificationsModule],
})
export class AppModule {}
