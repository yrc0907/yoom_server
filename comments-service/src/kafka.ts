import { Kafka } from 'kafkajs';

const KAFKA_BROKERS = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');

export const kafka = new Kafka({
  clientId: 'yoom-comments-service',
  brokers: KAFKA_BROKERS,
});

export const TOPIC_COMMENTS = 'comments';
