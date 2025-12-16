const seenContent = new Set<string>();
const seenUrl = new Set<string>();

export function seenUrlOnce(url: string): boolean {
  if (seenUrl.has(url)) return true;
  seenUrl.add(url);
  return false;
}

export function seenContentOnce(contentHash: string): boolean {
  if (!contentHash) return false;
  if (seenContent.has(contentHash)) return true;
  seenContent.add(contentHash);
  return false;
}
