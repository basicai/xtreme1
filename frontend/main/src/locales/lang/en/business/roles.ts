export default {
  roles: 'Roles',
  count: 'Count',
  roleType: 'Role Type',
  des: 'Create, edit, and manage roles in your organization. You can create a role based on an existing role or role template by enabling or disabling privileges as needed.',
  searchRole: 'Search for role name',
  default: 'Default',
  customize: 'Customize',
  create: {
    createRole: 'Create Role',
    editRole: 'Edit Role',
    roleName: 'Role Name',
    duplicate: 'Duplicate role name',
    description: 'Description',
    rolePrivileges: 'Role Privileges',
    expandAll: 'Expand all',
    collapseAll: 'Collapse all',
    import: 'Import',
    importFrom: 'Import From',
    importRole: 'Import from existing role',
    selected: 'selected',
    enableAll: 'Enable All',
  },
  delete: {
    deleteRole: 'Delete Role',
    want: 'Want to Delete Role ',
    deleted: 'All data regarding this role will be deleted',
  },
  privileges: {
    view: 'View',
    create: 'Create',
    edit: 'Edit',
    delete: 'Delete',
    export: 'Export',
    annotate: 'Annotate',
    invite: 'Invite',
    remove: 'Remove',
    manage: 'Manage',
  },
  function: {
    functionPrivileges: 'Function privileges',
    dataset: {
      dataset: 'Dataset',
      viewDesc:
        'Allow member to view all types of dataset and their data and ontology (classes & classifications) inside',
      createDesc:
        'Allow member to create all types of dataset, and upload data create ontology(classes & classifications) inside',
      editDesc:
        'Allow member to rename dataset and to modify data, ontology inside a dataset. Privileges of data modification includes upload data, frame a series, ungroup a series, merge to series data',
      exportDesc: 'Allow member to export raw data and annotation result',
      deleteDesc:
        'Allow member to delete dataset and data, ontology(classes & classifications) inside',
      annotateDesc: 'Allow member to annotate data',
    },
    ontologyCenter: {
      ontologyCenter: 'Ontology Center',
      viewDesc: 'Allow member to view ontologies and to copy ontologies from dataset',
      createDesc: 'Allow member to create ontologies',
      editDesc:
        'Allow member to edit ontologies and to save ontology from dataset to ontology center',
      deleteDesc: 'Allow member to delete ontologies',
    },
    model: {
      modelRun: 'Model Run',
      modelRunDesc: 'Allow member to run all Basic AI build-in models in dataset',
      modelTool: 'Model-assist Tool',
      modelToolDesc: 'Allow member to run all Basic AI build-in models in annotation tools',
    },
  },
  admin: {
    administrativePrivileges: 'Administrative privileges',
    member: {
      member: 'Member',
      viewDesc: 'Allow member to view all members of this team',
      inviteDesc: 'Allow member to invite member to this team',
      removeDesc: 'Allow member to remove existing member and cancel invitation for pending member',
      manageDesc: 'Allow member to manage team members, includes tag member',
    },
    groups: {
      groups: 'Groups',
      viewAll: 'View All',
      viewAllDesc: 'Allow member to view all groups in this team',
      createDesc: 'Allow member to create group in this team',
      addMembers: 'Add Members in all groups',
      addMembersDesc: 'Allow member to add members in all groups',
      removeMembers: 'Remove Members in all groups',
      removeMembersDesc: 'Allow member to remove members in all groups',
      disband: 'Disband  all groups',
      disbandDesc: 'Allow member to disband all groups',
    },
    roles: {
      roles: 'Roles',
      viewDesc: 'Allow member to create role',
      createDesc: 'Allow member to create role',
      assignRole: 'Assign Role',
      assignRoleDesc: 'Allow member to assign role to other members',
      editDesc: 'Allow member to edit customized role',
      deleteDesc: 'Allow member to delete customized role',
    },
  },
};