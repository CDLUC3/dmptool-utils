import { AnyAnswerType } from '@dmptool/types';

// Interfaces to describe the type of results we are pulling from the MySQL database

export interface LoadPlanInfo {
  id: number;
  dmpId: string;
  projectId: number;
  versionedTemplateId: number;
  createdById: number;
  created: string;
  modifiedById: number;
  modified: string;
  title: string;
  status: string;
  visibility: string;
  featured: boolean;
  registeredBy?: number;
  registered?: string;
  languageId: string;
}

export interface LoadProjectInfo {
  id: number;
  title: string;
  abstractText?: string;
  startDate?: string;
  endDate?: string;
  dmptool_research_domain?: string;
}

export interface LoadFundingInfo {
  id: number;
  uri?: string;
  name: string;
  status: string;
  grantId?: string;
  funderProjectNumber?: string;
  funderOpportunityNumber?: string;
}

export interface LoadMemberInfo {
  id: number;
  uri?: string;
  name?: string;
  email?: string;
  givenName?: string;
  surName?: string;
  orcid?: string;
  isPrimaryContact: boolean;
  roles: string;
}

export interface LoadDatasetInfo {
  title: string;
  type: string;
  personal_data: string;
  sensitive_data: string;
  dataset_id: {
    identifier: string;
    type: string;
  }
}

export interface LoadRelatedWorkInfo {
  identifier: string;
  workType?: string;
}

export interface LoadNarrativeQuestionInfo {
  questionId: number;
  questionText: string;
  questionOrder: number;
  answerId: number;
  answerJSON: AnyAnswerType;
}

export interface LoadNarrativeSectionInfo {
  sectionId: number;
  sectionTitle: string;
  sectionDescription?: string;
  sectionOrder: number;
  question: LoadNarrativeQuestionInfo[];
}

export interface LoadNarrativeInfo {
  templateId: number;
  templateTitle: string;
  templateDescription?: string;
  templateVersion?: string;
  section: LoadNarrativeSectionInfo[];
}

// The RDA Common Standard JSON schema relies heavily on internal pointers like $ref and
// the available json-schema-to-typescript have an extremely hard time dealing with them.
//
// So we need to manually define the types that we need to use.
export interface RDACommonStandardAffiliation {
  name: string;
  affiliation_id?: {
    identifier: string;
    type: string;
  }
}

export interface RDACommonStandardContact {
  name: string;
  mbox: string;
  contact_id: {
    identifier: string;
    type: string;
  }[],
  affiliation?: RDACommonStandardAffiliation[];
}

export interface RDACommonStandardContributor {
  contributor_id: {
    identifier: string;
    type: string;
  }[],
  name: string;
  affiliation?: RDACommonStandardAffiliation[];
  role: string[];
}

export interface RDACommonStandardProject {
  title: string;
  description?: string;
  start?: string;
  end?: string;
  project_id: {
    identifier: string;
    type: string;
  }[]
  funding?: {
    name: string;
    funding_status: string;
    grant_id?: {
      identifier: string;
      type: string;
    };
    funder_id?: {
      identifier: string;
      type: string;
    }
  }[]
}

export interface RDACommonStandardRelatedWork {
  identifier: string;
  type: string;
  relation_type?: string;
  resource_type?: string;
}

export interface RDACommonStandardDataset {
  title: string;
  type: string;
  description?: string;
  dataset_id: {
    identifier: string;
    type: string;
  },
  personal_data?: string;
  sensitive_data?: string;
  data_quality_assurance?: string[];
  is_reused?: boolean,
  issued?: string;
  keyword?: string[];
  language?: string;
  metadata?: {
    description?: string;
    language: string;
    metadata_standard_id: {
      identifier: string;
      type: string;
    }[]
  }[],
  preservation_statement?: string;
  security_and_privacy?: {
    title: string;
    description: string;
  }[],
  alternate_identifier?: {
    identifier: string;
    type: string;
  }[],
  technical_resource?: {
    name: string;
    description?: string;
    technical_resource_id: {
      identifier: string;
      type: string;
    }[]
  }[],
  distribution?: RDACommonStandardDistribution[];
}

export interface RDACommonStandardDistribution {
  title: string;
  description?: string;
  access_url?: string;
  download_url?: string;
  byte_size?: number;
  format?: string[];
  data_access: string;
  issued?: string;
  license?: {
    license_ref: string;
    start_date?: string;
  }[],
  host: RDACommonStandardHost;
}

export interface RDACommonStandardHost {
  title: string;
  description?: string;
  url: string;
  host_id: {
    identifier: string;
    type: string;
  }[],
  availability?: string;
  backup_frequency?: string;
  backup_type?: string;
  certified_with?: string;
  geo_location?: string;
  pid_system?: string[];
  storage_type?: string;
  support_versioning?: string;
}

export interface RDACommonStandardIdentifierType {
  identifier: string;
  type: StandardIdentifierType;
}

export enum StandardIdentifierType {
  ARK = 'ark',
  DOI = 'doi',
  HANDLE = 'handle',
  ROR = 'ror',
  URL = 'url',
  OTHER = 'other'
}

export interface DMPExtensionFunderProjectType {
  project_id: {
    identifier: string;
    type: StandardIdentifierType;
  };
  funder_id: {
    identifier: string;
    type: StandardIdentifierType;
  };
  project_identifier: {
    identifier: string;
    type: StandardIdentifierType;
  }
}

export interface DMPExtensionFunderOpportunityType {
  project_id: {
    identifier: string;
    type: StandardIdentifierType;
  };
  funder_id: {
    identifier: string;
    type: StandardIdentifierType;
  };
  opportunity_identifier: {
    identifier: string;
    type: StandardIdentifierType;
  }
}

interface DMPExtensionNarrativeAnswer {
  id: number;
  json: AnyAnswerType;
}

interface DMPExtensionNarrativeQuestion {
  id: number;
  text: string;
  order: number;
  answer?: DMPExtensionNarrativeAnswer
}

interface DMPExtensionNarrativeSection {
  id: number;
  title: string;
  description?: string;
  order: number;
  question: DMPExtensionNarrativeQuestion[]
}

export interface DMPExtensionNarrative {
  id: number;
  title: string;
  description?: string;
  version?: string;
  section: DMPExtensionNarrativeSection[]
}
