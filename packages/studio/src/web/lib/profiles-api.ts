import { apiGet } from './api';

export interface ProfilesResponse {
  readonly active: string | null;
  readonly profiles: Readonly<Record<string, unknown>>;
}

export const profilesQueryKey = ['profiles'] as const;
export const fetchProfiles = (): Promise<ProfilesResponse> => apiGet<ProfilesResponse>('/api/profiles');
