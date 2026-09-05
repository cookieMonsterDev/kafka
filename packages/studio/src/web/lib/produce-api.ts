import type {
  BurstRequestInput,
  BurstStartResponse,
  ProduceRequest,
  ProduceResponse,
} from '../../shared/contracts/produce';
import { apiSend } from './api';

export function produceMessages(input: ProduceRequest): Promise<ProduceResponse> {
  return apiSend('POST', '/api/produce', input);
}

export function startBurst(input: BurstRequestInput): Promise<BurstStartResponse> {
  return apiSend('POST', '/api/produce/burst', input);
}

export function burstProgressUrl(jobId: string): string {
  return `/api/produce/burst/${encodeURIComponent(jobId)}`;
}

export function cancelBurst(jobId: string): Promise<{ jobId: string }> {
  return apiSend('DELETE', burstProgressUrl(jobId));
}
