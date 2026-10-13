/** The wire shape carried over Redis. Keep it small and boring — it is JSON. */
export interface LearnNotification {
  userId: number;
  title: string;
  /** Epoch ms, stamped by the publishing process so you can see cross-pod lag. */
  publishedAt: number;
  /** Which process published it. Makes PHASE 4 legible. */
  publishedBy: string;
}
