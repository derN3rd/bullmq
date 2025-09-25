/*eslint-env node */
'use strict';

import { QueueBase } from './queue-base';
import { Job } from './job';
import { clientCommandMessageReg, QUEUE_EVENT_SUFFIX } from '../utils';
import { JobState, JobType } from '../types';
import { JobJsonRaw, Metrics } from '../interfaces';

/**
 * Provides different getters for different aspects of a queue.
 */
export class QueueGetters<JobBase extends Job = Job> extends QueueBase {
  getJob(jobId: string): Promise<JobBase | undefined> {
    return this.Job.fromId(this, jobId) as Promise<JobBase>;
  }

  private commandByType(
    types: JobType[],
    count: boolean,
    callback: (key: string, dataType: string) => void,
  ) {
    return types.map((type: string) => {
      type = type === 'waiting' ? 'wait' : type; // alias

      const key = this.toKey(type);

      switch (type) {
        case 'completed':
        case 'failed':
        case 'delayed':
        case 'prioritized':
        case 'repeat':
        case 'waiting-children':
          return callback(key, count ? 'zcard' : 'zrange');
        case 'active':
        case 'wait':
        case 'paused':
          return callback(key, count ? 'llen' : 'lrange');
      }
    });
  }

  private sanitizeJobTypes(types: JobType[] | JobType | undefined): JobType[] {
    const currentTypes = typeof types === 'string' ? [types] : types;

    if (Array.isArray(currentTypes) && currentTypes.length > 0) {
      const sanitizedTypes = [...currentTypes];

      if (sanitizedTypes.indexOf('waiting') !== -1) {
        sanitizedTypes.push('paused');
      }

      return [...new Set(sanitizedTypes)];
    }

    return [
      'active',
      'completed',
      'delayed',
      'failed',
      'paused',
      'prioritized',
      'waiting',
      'waiting-children',
    ];
  }

  /**
    Returns the number of jobs waiting to be processed. This includes jobs that are
    "waiting" or "delayed" or "prioritized" or "waiting-children".
  */
  async count(): Promise<number> {
    const count = await this.getJobCountByTypes(
      'waiting',
      'paused',
      'delayed',
      'prioritized',
      'waiting-children',
    );

    return count;
  }

  /**
   * Returns the time to live for a rate limited key in milliseconds.
   * @param maxJobs - max jobs to be considered in rate limit state. If not passed
   * it will return the remaining ttl without considering if max jobs is excedeed.
   * @returns -2 if the key does not exist.
   * -1 if the key exists but has no associated expire.
   * @see {@link https://redis.io/commands/pttl/}
   */
  async getRateLimitTtl(maxJobs?: number): Promise<number> {
    return this.scripts.getRateLimitTtl(maxJobs);
  }

  /**
   * Get jobId that starts debounced state.
   * @deprecated use getDeduplicationJobId method
   *
   * @param id - debounce identifier
   */
  async getDebounceJobId(id: string): Promise<string | null> {
    const client = await this.client;

    return client.get(`${this.keys.de}:${id}`);
  }

  /**
   * Get jobId from deduplicated state.
   *
   * @param id - deduplication identifier
   */
  async getDeduplicationJobId(id: string): Promise<string | null> {
    const client = await this.client;

    return client.get(`${this.keys.de}:${id}`);
  }

  /**
   * Job counts by type
   *
   * Queue#getJobCountByTypes('completed') =\> completed count
   * Queue#getJobCountByTypes('completed,failed') =\> completed + failed count
   * Queue#getJobCountByTypes('completed', 'failed') =\> completed + failed count
   * Queue#getJobCountByTypes('completed', 'waiting', 'failed') =\> completed + waiting + failed count
   */
  async getJobCountByTypes(...types: JobType[]): Promise<number> {
    const result = await this.getJobCounts(...types);
    return Object.values(result).reduce((sum, count) => sum + count, 0);
  }

  /**
   * Returns the job counts for each type specified or every list/set in the queue by default.
   *
   * @returns An object, key (type) and value (count)
   */
  async getJobCounts(...types: JobType[]): Promise<{
    [index: string]: number;
  }> {
    const currentTypes = this.sanitizeJobTypes(types);

    const responses = await this.scripts.getCounts(currentTypes);

    const counts: { [index: string]: number } = {};
    responses.forEach((res, index) => {
      counts[currentTypes[index]] = res || 0;
    });

    return counts;
  }

  /**
   * Get current job state.
   *
   * @param jobId - job identifier.
   * @returns Returns one of these values:
   * 'completed', 'failed', 'delayed', 'active', 'waiting', 'waiting-children', 'unknown'.
   */
  getJobState(jobId: string): Promise<JobState | 'unknown'> {
    return this.scripts.getState(jobId);
  }

  /**
   * Returns the number of jobs in completed status.
   */
  getCompletedCount(): Promise<number> {
    return this.getJobCountByTypes('completed');
  }

  /**
   * Returns the number of jobs in failed status.
   */
  getFailedCount(): Promise<number> {
    return this.getJobCountByTypes('failed');
  }

  /**
   * Returns the number of jobs in delayed status.
   */
  getDelayedCount(): Promise<number> {
    return this.getJobCountByTypes('delayed');
  }

  /**
   * Returns the number of jobs in active status.
   */
  getActiveCount(): Promise<number> {
    return this.getJobCountByTypes('active');
  }

  /**
   * Returns the number of jobs in prioritized status.
   */
  getPrioritizedCount(): Promise<number> {
    return this.getJobCountByTypes('prioritized');
  }

  /**
   * Returns the number of jobs per priority.
   */
  async getCountsPerPriority(priorities: number[]): Promise<{
    [index: string]: number;
  }> {
    const uniquePriorities = [...new Set(priorities)];
    const responses = await this.scripts.getCountsPerPriority(uniquePriorities);

    const counts: { [index: string]: number } = {};
    responses.forEach((res, index) => {
      counts[`${uniquePriorities[index]}`] = res || 0;
    });

    return counts;
  }

  /**
   * Returns the number of jobs in waiting or paused statuses.
   */
  getWaitingCount(): Promise<number> {
    return this.getJobCountByTypes('waiting');
  }

  /**
   * Returns the number of jobs in waiting-children status.
   */
  getWaitingChildrenCount(): Promise<number> {
    return this.getJobCountByTypes('waiting-children');
  }

  /**
   * Returns the jobs that are in the "waiting" status.
   * @param start - zero based index from where to start returning jobs.
   * @param end - zero based index where to stop returning jobs.
   */
  getWaiting(start = 0, end = -1): Promise<JobBase[]> {
    return this.getJobs(['waiting'], start, end, true);
  }

  /**
   * Returns the jobs that are in the "waiting-children" status.
   * I.E. parent jobs that have at least one child that has not completed yet.
   * @param start - zero based index from where to start returning jobs.
   * @param end - zero based index where to stop returning jobs.
   */
  getWaitingChildren(start = 0, end = -1): Promise<JobBase[]> {
    return this.getJobs(['waiting-children'], start, end, true);
  }

  /**
   * Returns the jobs that are in the "active" status.
   * @param start - zero based index from where to start returning jobs.
   * @param end - zero based index where to stop returning jobs.
   */
  getActive(start = 0, end = -1): Promise<JobBase[]> {
    return this.getJobs(['active'], start, end, true);
  }

  /**
   * Returns the jobs that are in the "delayed" status.
   * @param start - zero based index from where to start returning jobs.
   * @param end - zero based index where to stop returning jobs.
   */
  getDelayed(start = 0, end = -1): Promise<JobBase[]> {
    return this.getJobs(['delayed'], start, end, true);
  }

  /**
   * Returns the jobs that are in the "prioritized" status.
   * @param start - zero based index from where to start returning jobs.
   * @param end - zero based index where to stop returning jobs.
   */
  getPrioritized(start = 0, end = -1): Promise<JobBase[]> {
    return this.getJobs(['prioritized'], start, end, true);
  }

  /**
   * Returns the jobs that are in the "completed" status.
   * @param start - zero based index from where to start returning jobs.
   * @param end - zero based index where to stop returning jobs.
   */
  getCompleted(start = 0, end = -1): Promise<JobBase[]> {
    return this.getJobs(['completed'], start, end, false);
  }

  /**
   * Returns the jobs that are in the "failed" status.
   * @param start - zero based index from where to start returning jobs.
   * @param end - zero based index where to stop returning jobs.
   */
  getFailed(start = 0, end = -1): Promise<JobBase[]> {
    return this.getJobs(['failed'], start, end, false);
  }

  /**
   * Returns the qualified job ids and the raw job data (if available) of the
   * children jobs of the given parent job.
   * It is possible to get either the already processed children, in this case
   * an array of qualified job ids and their result values will be returned,
   * or the pending children, in this case an array of qualified job ids will
   * be returned.
   * A qualified job id is a string representing the job id in a given queue,
   * for example: "bull:myqueue:jobid".
   *
   * @param parentId - The id of the parent job
   * @param type - "processed" | "pending"
   * @param opts - Options for the query.
   *
   * @returns an object with the following shape:
   * `{ items: { id: string, v?: any, err?: string } [], jobs: JobJsonRaw[], total: number}`
   */
  async getDependencies(
    parentId: string,
    type: 'processed' | 'pending',
    start: number,
    end: number,
  ): Promise<{
    items: { id: string; v?: any; err?: string }[];
    jobs: JobJsonRaw[];
    total: number;
  }> {
    const key = this.toKey(
      type == 'processed'
        ? `${parentId}:processed`
        : `${parentId}:dependencies`,
    );
    const { items, total, jobs } = await this.scripts.paginate(key, {
      start,
      end,
      fetchJobs: true,
    });
    return {
      items,
      jobs,
      total,
    };
  }

  async getRanges(
    types: JobType[],
    start = 0,
    end = 1,
    asc = false,
  ): Promise<string[]> {
    const multiCommands: string[] = [];

    this.commandByType(types, false, (key, command) => {
      switch (command) {
        case 'lrange':
          multiCommands.push('lrange');
          break;
        case 'zrange':
          multiCommands.push('zrange');
          break;
      }
    });

    const responses = await this.scripts.getRanges(types, start, end, asc);

    let results: string[] = [];

    responses.forEach((response: string[], index: number) => {
      const result = response || [];

      if (asc && multiCommands[index] === 'lrange') {
        results = results.concat(result.reverse());
      } else {
        results = results.concat(result);
      }
    });

    return [...new Set(results)];
  }

  /**
   * Returns the jobs that are on the given statuses (note that JobType is synonym for job status)
   * @param types - the statuses of the jobs to return.
   * @param start - zero based index from where to start returning jobs.
   * @param end - zero based index where to stop returning jobs.
   * @param asc - if true, the jobs will be returned in ascending order.
   */
  async getJobs(
    types?: JobType[] | JobType,
    start = 0,
    end = -1,
    asc = false,
  ): Promise<JobBase[]> {
    const currentTypes = this.sanitizeJobTypes(types);

    const jobIds = await this.getRanges(currentTypes, start, end, asc);

    return Promise.all(
      jobIds.map(jobId => this.Job.fromId(this, jobId) as Promise<JobBase>),
    );
  }

  /**
   * Returns the logs for a given Job.
   * @param jobId - the id of the job to get the logs for.
   * @param start - zero based index from where to start returning jobs.
   * @param end - zero based index where to stop returning jobs.
   * @param asc - if true, the jobs will be returned in ascending order.
   */
  async getJobLogs(
    jobId: string,
    start = 0,
    end = -1,
    asc = true,
  ): Promise<{ logs: string[]; count: number }> {
    const client = await this.client;
    const multi = client.multi();

    const logsKey = this.toKey(jobId + ':logs');
    if (asc) {
      multi.lrange(logsKey, start, end);
    } else {
      multi.lrange(logsKey, -(end + 1), -(start + 1));
    }
    multi.llen(logsKey);
    const result = (await multi.exec()) as [[Error, [string]], [Error, number]];
    if (!asc) {
      result[0][1].reverse();
    }
    return {
      logs: result[0][1],
      count: result[1][1],
    };
  }

  private async baseGetClients(matcher: (name: string) => boolean): Promise<
    {
      [index: string]: string;
    }[]
  > {
    const client = await this.client;
    try {
      const clients = (await client.client('LIST')) as string;
      const list = this.parseClientList(clients, matcher);
      return list;
    } catch (err) {
      if (!clientCommandMessageReg.test((<Error>err).message)) {
        throw err;
      }

      return [{ name: 'GCP does not support client list' }];
    }
  }

  /**
   * Get the worker list related to the queue. i.e. all the known
   * workers that are available to process jobs for this queue.
   * Note: GCP does not support SETNAME, so this call will not work
   *
   * @returns - Returns an array with workers info.
   */
  getWorkers(): Promise<
    {
      [index: string]: string;
    }[]
  > {
    const unnamedWorkerClientName = `${this.clientName()}`;
    const namedWorkerClientName = `${this.clientName()}:w:`;

    const matcher = (name: string) =>
      name &&
      (name === unnamedWorkerClientName ||
        name.startsWith(namedWorkerClientName));

    return this.baseGetClients(matcher);
  }

  /**
   * Returns the current count of workers for the queue.
   *
   * getWorkersCount(): Promise<number>
   *
   */
  async getWorkersCount(): Promise<number> {
    const workers = await this.getWorkers();
    return workers.length;
  }

  /**
   * Get queue events list related to the queue.
   * Note: GCP does not support SETNAME, so this call will not work
   *
   * @deprecated do not use this method, it will be removed in the future.
   *
   * @returns - Returns an array with queue events info.
   */
  async getQueueEvents(): Promise<
    {
      [index: string]: string;
    }[]
  > {
    const clientName = `${this.clientName()}${QUEUE_EVENT_SUFFIX}`;
    return this.baseGetClients((name: string) => name === clientName);
  }

  getMetricsKeys = (type: 'completed' | 'failed') => {
    return {
      metricsKey: this.toKey(`metrics:${type}`),
      dataKey: this.toKey(`metrics:${type}:data`),
    };
  };

  /**
   * Get queue metrics related to the queue.
   *
   * This method returns the gathered metrics for the queue.
   * The metrics are represented as an array of job counts
   * per unit of time (1 minute).
   *
   * @param start - Start point of the metrics, where 0
   * is the newest point to be returned.
   * @param end - End point of the metrics, where -1 is the
   * oldest point to be returned.
   *
   * @returns - Returns an object with queue metrics.
   */
  async getMetrics(
    type: 'completed' | 'failed',
    start = 0,
    end = -1,
  ): Promise<Metrics> {
    const client = await this.client;
    const { metricsKey, dataKey } = this.getMetricsKeys(type);

    const multi = client.multi();
    multi.hmget(metricsKey, 'count', 'prevTS', 'prevCount');
    multi.lrange(dataKey, start, end);
    multi.llen(dataKey);

    const [hmget, range, len] = (await multi.exec()) as [
      [Error, [string, string, string]],
      [Error, []],
      [Error, number],
    ];
    const [err, [count, prevTS, prevCount]] = hmget;
    const [err2, data] = range;
    const [err3, numPoints] = len;
    if (err || err2) {
      throw err || err2 || err3;
    }

    return {
      meta: {
        count: parseInt(count || '0', 10),
        prevTS: parseInt(prevTS || '0', 10),
        prevCount: parseInt(prevCount || '0', 10),
      },
      data,
      count: numPoints,
    };
  }

  /**
   * Get execution time percentiles for all jobs (completed and failed combined).
   * This method does not modify the data and is safe to call from multiple
   * Prometheus exporters simultaneously.
   *
   * @param timeWindowSeconds - How many seconds back to look (default: 900 = 15 minutes)
   * @param bucketSizeSeconds - Bucket size in seconds (should match your metrics configuration, default: 15)
   * @returns Object with p50, p95, p99 percentiles and sample count
   */
  async getExecutionTimePercentiles(
    timeWindowSeconds = 900,
    bucketSizeSeconds = 15,
  ): Promise<{ p50: number; p95: number; p99: number; count: number }> {
    const [p50, p95, p99, count] = await this.scripts.getExecutionPercentiles(
      timeWindowSeconds,
      bucketSizeSeconds,
    );

    return { p50, p95, p99, count };
  }

  private parseClientList(list: string, matcher: (name: string) => boolean) {
    const lines = list.split(/\r?\n/);
    const clients: { [index: string]: string }[] = [];

    lines.forEach((line: string) => {
      const client: { [index: string]: string } = {};
      const keyValues = line.split(' ');
      keyValues.forEach(function (keyValue) {
        const index = keyValue.indexOf('=');
        const key = keyValue.substring(0, index);
        const value = keyValue.substring(index + 1);
        client[key] = value;
      });
      const name = client['name'];
      if (matcher(name)) {
        client['name'] = this.name;
        client['rawname'] = name;
        clients.push(client);
      }
    });
    return clients;
  }

  /**
   * Export the metrics for the queue in the Prometheus format.
   * Automatically exports all the counts returned by getJobCounts().
   * Includes the counter of "completed" and "failed" jobs if metrics are enabled.
   * Includes job execution time percentiles if timing collection is enabled.
   *
   * @param globalVariables - Additional labels to add to all metrics
   * @returns - Returns a string with the metrics in the Prometheus format.
   *
   * @see {@link https://prometheus.io/docs/instrumenting/exposition_formats/}
   *
   **/
  async exportPrometheusMetrics(
    globalVariables?: Record<string, string>,
    timingMetricsOptions?: {
      /**
       * How many seconds back to look (default: 30s).
       * Should be 2x the bucket size to handle Prometheus scraping timing variations.
       */
      timeWindowSeconds?: number;
      /**
       * Bucket size in seconds (default: 15s).
       * Must match the timingBucketSeconds configured in your worker metrics options.
       */
      bucketSizeSeconds?: number;
    },
  ): Promise<string> {
    const counts = await this.getJobCounts();
    const metrics: string[] = [];

    // Match the test's expected HELP text
    metrics.push(
      '# HELP bullmq_job_count Number of jobs in the queue by state',
    );
    metrics.push('# TYPE bullmq_job_count gauge');

    const variables = !globalVariables
      ? ''
      : Object.keys(globalVariables).reduce(
          (acc, curr) => `${acc}, ${curr}="${globalVariables[curr]}"`,
          '',
        );

    for (const [state, count] of Object.entries(counts)) {
      metrics.push(
        `bullmq_job_count{queue="${this.name}", state="${state}"${variables}} ${count}`,
      );
    }

    /**
     * Get the counter of completed or failed jobs.
     *
     * Not reusing getMetrics method here, as it fetches multiple other things that we don't need.
     */
    const getMetricsCounter = async (type: 'completed' | 'failed') => {
      const client = await this.client;
      const { metricsKey } = this.getMetricsKeys(type);
      const count = await client.hget(metricsKey, 'count');

      return parseInt(count || '0', 10);
    };

    const completedMetricsCounter = await getMetricsCounter('completed');
    const failedMetricsCounter = await getMetricsCounter('failed');

    metrics.push(
      '# HELP bullmq_job_amount Number of jobs processed in the queue by state',
    );
    metrics.push('# TYPE bullmq_job_amount counter');
    metrics.push(
      `bullmq_job_amount{queue="${this.name}", state="completed"${variables}} ${completedMetricsCounter}`,
    );
    metrics.push(
      `bullmq_job_amount{queue="${this.name}", state="failed"${variables}} ${failedMetricsCounter}`,
    );

    // Add execution time percentiles if timing collection is enabled
    try {
      const bucketSize = timingMetricsOptions?.bucketSizeSeconds ?? 15;
      const timeWindow =
        timingMetricsOptions?.timeWindowSeconds ?? bucketSize * 3;

      const timings = await this.getExecutionTimePercentiles(
        timeWindow,
        bucketSize,
      );

      metrics.push(
        '# HELP bullmq_job_duration_percentiles Job execution time percentiles in milliseconds',
      );
      metrics.push('# TYPE bullmq_job_duration_percentiles gauge');

      // Add percentile metrics
      metrics.push(
        `bullmq_job_duration_percentiles{queue="${this.name}", percentile="50"${variables}} ${timings.p50}`,
      );
      metrics.push(
        `bullmq_job_duration_percentiles{queue="${this.name}", percentile="95"${variables}} ${timings.p95}`,
      );
      metrics.push(
        `bullmq_job_duration_percentiles{queue="${this.name}", percentile="99"${variables}} ${timings.p99}`,
      );

      // Add sample count for observability
      metrics.push(
        '# HELP bullmq_job_duration_samples Number of timing samples used for percentile calculation',
      );
      metrics.push('# TYPE bullmq_job_duration_samples gauge');
      metrics.push(
        `bullmq_job_duration_samples{queue="${this.name}"${variables}} ${timings.count}`,
      );
    } catch (error) {
      // Log error but don't fail the entire metrics export
      console.warn(
        `Failed to collect timing metrics for queue ${this.name}:`,
        error,
      );
    }

    return metrics.join('\n');
  }
}
