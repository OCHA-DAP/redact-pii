import { get } from 'lodash';
import DLP from '@google-cloud/dlp';
import { defaultInfoTypes, GoogleDLPRedactorOptions, MAX_DLP_CONTENT_LENGTH } from './GoogleDLPRedactor';
import { auth, Compute } from 'google-auth-library';

// a finding quote length that is too short (e.g. 1 char like "S") causes too many false replacements
const MIN_FINDING_QUOTE_LENGTH = 2;

const minLikelihood = 'LIKELIHOOD_UNSPECIFIED';
const maxFindings = 0;
const customInfoTypes = [
  {
    infoType: {
      name: 'URL'
    },
    regex: {
      pattern: '([^\\s:/?#]+):\\/\\/([^/?#\\s]*)([^?#\\s]*)(\\?([^#\\s]*))?(#([^\\s]*))?'
    }
  }
];

const likelihoodPriority: { [likelyHoodName: string]: number } = {
  EXCLUDE: -1,
  LIKELIHOOD_UNSPECIFIED: 0,
  VERY_UNLIKELY: 1,
  UNLIKELY: 2,
  POSSIBLE: 3,
  LIKELY: 4,
  VERY_LIKELY: 5
};

export const LIKELIHOOD_PRIORITY = likelihoodPriority;
const DEBUG = process.env.DEBUG === 'true';
const TRACE = process.env.TRACE === 'true';
const MAX_CONTENT_LENGTH = process.env.MAX_DLP_CONTENT_LENGTH
  ? parseInt(process.env.MAX_DLP_CONTENT_LENGTH)
  : MAX_DLP_CONTENT_LENGTH;
const MAX_PROMISES = process.env.MAX_PROMISES ? process.env.MAX_PROMISES : 5;

const includeQuote = true;

interface Finding {
  likelihood: string;
  quote: string;
  infoType: {
    name: string;
  };
  location: {
    byteRange: {
      start: string;
      end: string;
    };
  };
}

// finding location.byteRange.start and end are strings for some reason, so must convert to numbers
const getFindingStart = (finding: Finding) => Number(get(finding, 'location.byteRange.start', 0));
const getFindingEnd = (finding: Finding) => Number(get(finding, 'location.byteRange.end', 0));

/**
 * Remove overlapping findings which can cause messed up tokens.
 *
 * For example "My name is John D." will cause 3 findings:
 *  - PERSON_NAME for text "John S." at range 11-17
 *  - FIRST_NAME for text "John" at range 11-15
 *  - LAST_NAME for text "S." at range 15-17
 *
 * The FIRST_NAME and LAST_NAME findings overlap the first finding so there is no need to search for them
 */
function removeOverlappingFindings(findings: Finding[]): Finding[] {
  // early return if only have 0 or 1 findings
  if (findings.length <= 1) {
    return findings;
  }

  // sort findings by ascending start
  findings.sort((a, b) => getFindingStart(a) - getFindingStart(b));

  // remove findings that overlap (but keep the one with higher likelihood)
  const resultFindings = [findings[0]];
  for (let i = 1; i < findings.length; i++) {
    const current = findings[i];
    const previous = resultFindings[resultFindings.length - 1];

    // when findings overlap, keep the one with the higher likelihood
    if (getFindingStart(current) < getFindingEnd(previous)) {
      if (likelihoodPriority[current.likelihood] > likelihoodPriority[previous.likelihood]) {
        resultFindings[resultFindings.length - 1] = current;
      }
    } else {
      // no overlap
      resultFindings.push(current);
    }
  }

  return resultFindings;
}

export interface RedactorOutput {
  config: {
    infoTypes: string[];
  };
  metrics: {
    findingsCap: number;
    maxFindings: number;
  };
  text: string;
  findings: Finding[];
}

/** @public */
export interface HDXGoogleDLPRedactorOptions extends GoogleDLPRedactorOptions {
  /** Array of custom DLP info type names to include */
  customInfoTypes?: string[];
}

/** @public */
export class HDXGoogleDLPRedactor {
  dlpClient: typeof DLP.DlpServiceClient;

  constructor(private opts: HDXGoogleDLPRedactorOptions = {}) {
    this.dlpClient = new DLP.DlpServiceClient();
  }
  async redactAsync(textToRedact: string, filter?: any): Promise<RedactorOutput> {
    // default batch size is MAX_DLP_CONTENT_LENGTH/2 because some unicode characters can take more than 1 byte
    // and its difficult to get a substring of a desired target length in bytes
    const maxContentSize = this.opts.maxContentSizeForBatch || MAX_CONTENT_LENGTH / 2;

    if (TRACE) {
      console.log(
        `text length[${textToRedact.length}] > maxContentSize[${maxContentSize}] && !disableAutoBatch[${!this.opts
          .disableAutoBatchWhenContentSizeExceedsLimit}]`
      );
    }
    if (textToRedact.length > maxContentSize && !this.opts.disableAutoBatchWhenContentSizeExceedsLimit) {
      let batchPromises = [];
      let batchStartIndex = 0;
      let batchResults: RedactorOutput[] = [];
      while (batchStartIndex < textToRedact.length) {
        const batchEndIndex = batchStartIndex + maxContentSize;
        const batchText = textToRedact.substring(batchStartIndex, batchEndIndex);
        if (TRACE) {
          console.log(`Testing batch start[${batchStartIndex}] size[${maxContentSize}]`);
        }
        batchPromises.push(this.doRedactAsync(batchText, filter));
        batchStartIndex = batchEndIndex;
        if (batchPromises.length >= MAX_PROMISES) {
          batchResults.push(...(await Promise.all(batchPromises)));
          batchPromises = [];
        }
      }
      if (TRACE) {
        console.log(`Waiting for DLP...`);
      }
      // const batchResults = await Promise.all(batchPromises);
      batchResults.push(...(await Promise.all(batchPromises)));
      if (TRACE) {
        console.log(`Response received, mapping ...`);
      }
      const findings = batchResults.map(f => f.findings).reduce((a, b) => [...a, ...b]);
      const text = batchResults.map(f => f.text).reduce((a, b) => a + b);
      const config = {
        infoTypes: batchResults.map(f => f.config.infoTypes).reduce((a, b) => b) //just get the last config, they are all the same
      };
      const maxFindings = batchResults.map(f => f.metrics.maxFindings).reduce((a, b) => Math.max(a, b));
      const findingsCap = batchResults.map(f => f.metrics.findingsCap).reduce((a, b) => a + b);
      if (TRACE) {
        console.log(`Return response`);
      }
      return {
        config: config,
        findings: findings,
        text: text,
        metrics: {
          findingsCap: findingsCap,
          maxFindings: maxFindings
        }
      };
    } else {
      return this.doRedactAsync(textToRedact, filter);
    }
  }

  getInspectConfig(): object {
    // Add only included infotypes
    const infoTypes = (this.opts.includeInfoTypes || []).map(infoTypeName => ({ name: infoTypeName }));
    const customInfoTypes = (this.opts.customInfoTypes || []).map(infoTypeName => ({
      infoType: { name: infoTypeName },
      storedType: { name: `projects/fifth-catcher-269319/storedInfoTypes/${infoTypeName}` }
    }));

    let inspectConfig = Object.assign(
      {
        infoTypes,
        customInfoTypes,
        minLikelihood,
        includeQuote,
        limits: {
          maxFindingsPerRequest: maxFindings
        }
      },
      this.opts.inspectConfig
    );
    if (TRACE) {
      console.log('Inspect config:');
      console.log(JSON.stringify(inspectConfig, null, 1));
    }

    return inspectConfig;
  }

  async doRedactAsync(textToRedact: string, filter?: any, replaceText = false): Promise<RedactorOutput> {
    const projectId = await this.dlpClient.getProjectId();

    let inspectConfig: any = this.getInspectConfig();
    const response = await this.dlpClient.inspectContent({
      parent: this.dlpClient.projectPath(projectId),
      inspectConfig: inspectConfig,
      item: { value: textToRedact }
    });
    const findings: Finding[] = response[0].result.findings;
    let filteredFindings = findings;
    if (findings.length > 0) {
      filteredFindings = findings.filter(a => {
        //Enable to debug filter
        if (TRACE) {
          console.log(
            `${a.infoType.name} ${filter[a.infoType.name]} <= ${LIKELIHOOD_PRIORITY[a.likelihood]} || ${!filter[
              a.infoType.name
            ]}`
          );
        }
        return filter[a.infoType.name] <= LIKELIHOOD_PRIORITY[a.likelihood] || !filter[a.infoType.name];
      });
      // this is necessary to prevent tokens getting messed up with other repeated partial tokens (e.g. "my name is PERLALALALALALALALALALALALALALALALALAL...")
      if (replaceText) {
        const findingsWithoutOverlaps = removeOverlappingFindings(filteredFindings);

        // sort findings by highest likelihood first
        findingsWithoutOverlaps.sort(function(a: any, b: any) {
          return likelihoodPriority[b.likelihood] - likelihoodPriority[a.likelihood];
        });

        // in order of highest likelihood replace finding with info type name
        findingsWithoutOverlaps.forEach((finding: any) => {
          let find = finding.quote;
          if (find !== finding.infoType.name && find.length >= MIN_FINDING_QUOTE_LENGTH) {
            let numSearches = 0;
            while (numSearches++ < 1000 && textToRedact.indexOf(find) >= 0) {
              textToRedact = textToRedact.replace(find, finding.infoType.name);
            }
          }
        });
      }
    }
    if (TRACE) {
      console.log(`Findings ${findings.length}`);
    }
    return {
      config: {
        infoTypes: inspectConfig.infoTypes
          // @ts-ignore
          .map(i => i.name)
          // @ts-ignore
          .concat(inspectConfig.customInfoTypes.map(i => i.infoType.name))
      },
      text: textToRedact,
      findings: filteredFindings,
      metrics: {
        maxFindings: findings.length,
        findingsCap: findings.length >= 2000 ? 1 : 0
      }
    };
  }

  async inspectStructured(headers: string[], rows: string[][], filter?: any): Promise<RedactorOutput> {
    // default batch size is MAX_DLP_CONTENT_LENGTH/2 because some unicode characters can take more than 1 byte
    // and its difficult to get a substring of a desired target length in bytes
    const maxContentSize = Math.trunc(50000 / headers.length / 2) - 1;

    if (TRACE) {
      console.log(`Number of rows[${rows.length}] > maxContentSize[${maxContentSize}]`);
    }
    if (rows.length > maxContentSize) {
      let batchPromises = [];
      let batchStartIndex = 0;
      let batchResults: RedactorOutput[] = [];
      while (batchStartIndex < rows.length) {
        const batchEndIndex = batchStartIndex + maxContentSize;
        const rowsBatch = rows.slice(batchStartIndex, batchEndIndex);
        if (TRACE) {
          console.log(`Testing batch start[${batchStartIndex}] size[${maxContentSize}]`);
        }
        batchPromises.push(this.doInspectStructured(headers, rowsBatch, filter));
        batchStartIndex = batchEndIndex;
        if (batchPromises.length >= MAX_PROMISES) {
          batchResults.push(...(await Promise.all(batchPromises)));
          batchPromises = [];
        }
      }
      if (TRACE) {
        console.log(`Waiting for DLP...`);
      }
      // const batchResults = await Promise.all(batchPromises);
      batchResults.push(...(await Promise.all(batchPromises)));
      if (TRACE) {
        console.log(`Response received, mapping ...`);
      }
      const findings = batchResults.map(f => f.findings).reduce((a, b) => [...a, ...b]);
      const text = batchResults.map(f => f.text).reduce((a, b) => a + b);
      const config = {
        infoTypes: batchResults.map(f => f.config.infoTypes).reduce((a, b) => b) //just get the last config, they are all the same
      };
      const maxFindings = batchResults.map(f => f.metrics.maxFindings).reduce((a, b) => Math.max(a, b));
      const findingsCap = batchResults.map(f => f.metrics.findingsCap).reduce((a, b) => a + b);
      if (TRACE) {
        console.log(`Return response`);
      }
      return {
        config: config,
        findings: findings,
        text: text,
        metrics: {
          findingsCap: findingsCap,
          maxFindings: maxFindings
        }
      };
    } else {
      return this.doInspectStructured(headers, rows, filter);
    }
  }
  async doInspectStructured(headers: string[], rows: string[][], filter?: any): Promise<RedactorOutput> {
    const projectId = await this.dlpClient.getProjectId();

    let inspectConfig: any = this.getInspectConfig();
    let inspectOptions = {
      // parent: this.dlpClient.projectPath(projectId),
      parent: `projects/${projectId}/locations/global`,
      inspectConfig: inspectConfig,
      item: {
        // value: textToRedact,
        table: {
          headers: headers.map(header => {
            return { name: header };
          }),
          rows: rows.map(row => {
            return {
              values: row.map(value => {
                return { string_value: value };
              })
            };
          })
        }
      }
    };
    if (TRACE) {
      console.log('$');
      console.log('$');
      console.log('$');
      console.log(JSON.stringify(inspectOptions, null, 2));
      console.log('$');
      console.log('$');
      console.log('$');
    }
    // const response = await this.dlpClient.inspectContent(inspectOptions);
    // load the JWT or UserRefreshClient from the keys
    // load the environment variable with our keys
    const keys = this.opts.clientOptions.credentials;

    const client: any = auth.fromJSON(keys);
    client.scopes = ['https://www.googleapis.com/auth/cloud-platform'];
    const url = `https://dlp.googleapis.com/v2/projects/${projectId}/content:inspect`;
    const res = await client.request({ url, method: 'POST', body: JSON.stringify(inspectOptions) });
    let response = res.data;

    if (TRACE) {
      console.log('*');
      console.log('*');
      console.log('*');
      console.log(JSON.stringify(response, null, 2));
      console.log('*');
      console.log('*');
      console.log('*');
    }

    let findings: Finding[] = response.result.findings;
    if (!findings) {
      findings = [];
    }
    let filteredFindings = findings;
    if (findings.length > 0) {
      filteredFindings = findings.filter(a => {
        //Enable to debug filter
        if (TRACE) {
          console.log(
            `${a.infoType.name} ${filter[a.infoType.name]} <= ${LIKELIHOOD_PRIORITY[a.likelihood]} || ${!filter[
              a.infoType.name
            ]}`
          );
        }
        return filter[a.infoType.name] <= LIKELIHOOD_PRIORITY[a.likelihood] || !filter[a.infoType.name];
      });
    }
    if (TRACE) {
      console.log(`Findings ${findings.length}`);
    }

    return {
      config: {
        infoTypes: inspectConfig.infoTypes
          // @ts-ignore
          .map(i => i.name)
          // @ts-ignore
          .concat(inspectConfig.customInfoTypes.map(i => i.infoType.name))
      },
      text: '',
      findings: filteredFindings,
      metrics: {
        maxFindings: findings.length,
        findingsCap: findings.length >= 2000 ? 1 : 0
      }
    };
  }
}
