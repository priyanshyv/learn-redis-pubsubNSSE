import { IsInt, IsString, IsNotEmpty, Min } from 'class-validator';

export class PublishNotificationDto {
  @IsInt()
  @Min(1)
  userId: number;

  @IsString()
  @IsNotEmpty()
  title: string;
}
