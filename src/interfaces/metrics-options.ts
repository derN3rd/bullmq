/**
 * Options for collecting queue metrics
 */
export interface MetricsOptions {
  /**
   * Enable gathering metrics for finished jobs.
   * Output refers to all finished jobs, completed or
   * failed.
   */
  maxDataPoints?: number;

  /**
   * Enable collection of job execution time metrics.
   * When enabled, execution times will be stored in Redis
   * for percentile calculations.
   *
   * @defaultValue false
   */
  collectTimings?: boolean;

  /**
   * Time bucket size in seconds for timing metrics collection.
   * Smaller buckets provide more granular data but use more memory.
   * Should match your Prometheus scraping interval.
   *
   * @defaultValue 15
   */
  timingBucketSeconds?: number;
}
