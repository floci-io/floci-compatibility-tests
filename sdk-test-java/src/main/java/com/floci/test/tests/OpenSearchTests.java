package com.floci.test.tests;

import com.floci.test.FlociTestGroup;
import com.floci.test.TestContext;
import com.floci.test.TestGroup;
import software.amazon.awssdk.services.opensearch.OpenSearchClient;
import software.amazon.awssdk.services.opensearch.model.*;

import java.util.List;
import java.util.Map;

@FlociTestGroup
public class OpenSearchTests implements TestGroup {

    @Override
    public String name() { return "opensearch"; }

    @Override
    public void run(TestContext ctx) {
        System.out.println("--- OpenSearch Tests ---");

        String domainName = "sdk-test-" + System.currentTimeMillis() % 100000;

        try (OpenSearchClient os = OpenSearchClient.builder()
                .endpointOverride(ctx.endpoint)
                .region(ctx.region)
                .credentialsProvider(ctx.credentials)
                .build()) {

            // 1. CreateDomain
            DomainStatus created;
            try {
                CreateDomainResponse resp = os.createDomain(CreateDomainRequest.builder()
                        .domainName(domainName)
                        .engineVersion("OpenSearch_2.11")
                        .clusterConfig(ClusterConfig.builder()
                                .instanceType(OpenSearchPartitionInstanceType.M5_LARGE_SEARCH)
                                .instanceCount(1)
                                .build())
                        .ebsOptions(EBSOptions.builder()
                                .ebsEnabled(true)
                                .volumeType(VolumeType.GP2)
                                .volumeSize(10)
                                .build())
                        .build());
                created = resp.domainStatus();
                ctx.check("OpenSearch CreateDomain",
                        created != null
                        && domainName.equals(created.domainName())
                        && created.arn() != null && created.arn().contains(domainName)
                        && !created.processing());
            } catch (Exception e) {
                ctx.check("OpenSearch CreateDomain", false, e);
                return;
            }

            // 2. CreateDomain duplicate → ResourceAlreadyExistsException
            try {
                os.createDomain(CreateDomainRequest.builder().domainName(domainName).build());
                ctx.check("OpenSearch CreateDomain duplicate fails", false);
            } catch (ResourceAlreadyExistsException e) {
                ctx.check("OpenSearch CreateDomain duplicate fails", true);
            } catch (Exception e) {
                ctx.check("OpenSearch CreateDomain duplicate fails", false, e);
            }

            // 3. DescribeDomain
            try {
                DescribeDomainResponse resp = os.describeDomain(
                        DescribeDomainRequest.builder().domainName(domainName).build());
                DomainStatus status = resp.domainStatus();
                ctx.check("OpenSearch DescribeDomain",
                        domainName.equals(status.domainName())
                        && "OpenSearch_2.11".equals(status.engineVersion())
                        && status.clusterConfig().instanceCount() == 1);
            } catch (Exception e) {
                ctx.check("OpenSearch DescribeDomain", false, e);
            }

            // 4. DescribeDomains (batch)
            try {
                DescribeDomainsResponse resp = os.describeDomains(
                        DescribeDomainsRequest.builder().domainNames(domainName).build());
                ctx.check("OpenSearch DescribeDomains",
                        resp.domainStatusList().size() == 1
                        && domainName.equals(resp.domainStatusList().get(0).domainName()));
            } catch (Exception e) {
                ctx.check("OpenSearch DescribeDomains", false, e);
            }

            // 5. ListDomainNames
            try {
                ListDomainNamesResponse resp = os.listDomainNames(
                        ListDomainNamesRequest.builder().build());
                boolean found = resp.domainNames().stream()
                        .anyMatch(d -> domainName.equals(d.domainName()));
                ctx.check("OpenSearch ListDomainNames", found);
            } catch (Exception e) {
                ctx.check("OpenSearch ListDomainNames", false, e);
            }

            // 6. ListDomainNames filtered by engine type
            try {
                ListDomainNamesResponse resp = os.listDomainNames(
                        ListDomainNamesRequest.builder().engineType(EngineType.OPEN_SEARCH).build());
                boolean found = resp.domainNames().stream()
                        .anyMatch(d -> domainName.equals(d.domainName()));
                ctx.check("OpenSearch ListDomainNames engineType filter", found);
            } catch (Exception e) {
                ctx.check("OpenSearch ListDomainNames engineType filter", false, e);
            }

            // 7. DescribeDomainConfig
            try {
                DescribeDomainConfigResponse resp = os.describeDomainConfig(
                        DescribeDomainConfigRequest.builder().domainName(domainName).build());
                DomainConfig cfg = resp.domainConfig();
                ctx.check("OpenSearch DescribeDomainConfig",
                        cfg.clusterConfig() != null
                        && cfg.clusterConfig().options() != null
                        && cfg.ebsOptions() != null);
            } catch (Exception e) {
                ctx.check("OpenSearch DescribeDomainConfig", false, e);
            }

            // 8. UpdateDomainConfig
            try {
                UpdateDomainConfigResponse resp = os.updateDomainConfig(
                        UpdateDomainConfigRequest.builder()
                                .domainName(domainName)
                                .clusterConfig(ClusterConfig.builder()
                                        .instanceCount(3)
                                        .build())
                                .build());
                ctx.check("OpenSearch UpdateDomainConfig",
                        resp.domainConfig().clusterConfig().options().instanceCount() == 3);
            } catch (Exception e) {
                ctx.check("OpenSearch UpdateDomainConfig", false, e);
            }

            // 9. DescribeDomain non-existent → ResourceNotFoundException
            try {
                os.describeDomain(DescribeDomainRequest.builder()
                        .domainName("nonexistent-xyz").build());
                ctx.check("OpenSearch DescribeDomain non-existent fails", false);
            } catch (ResourceNotFoundException e) {
                ctx.check("OpenSearch DescribeDomain non-existent fails", true);
            } catch (Exception e) {
                ctx.check("OpenSearch DescribeDomain non-existent fails", false, e);
            }

            // 10. AddTags
            try {
                os.addTags(AddTagsRequest.builder()
                        .arn(created.arn())
                        .tagList(
                                Tag.builder().key("env").value("test").build(),
                                Tag.builder().key("owner").value("sdk").build())
                        .build());
                ctx.check("OpenSearch AddTags", true);
            } catch (Exception e) {
                ctx.check("OpenSearch AddTags", false, e);
            }

            // 11. ListTags
            try {
                ListTagsResponse resp = os.listTags(
                        ListTagsRequest.builder().arn(created.arn()).build());
                boolean hasEnv = resp.tagList().stream()
                        .anyMatch(t -> "env".equals(t.key()) && "test".equals(t.value()));
                boolean hasOwner = resp.tagList().stream()
                        .anyMatch(t -> "owner".equals(t.key()));
                ctx.check("OpenSearch ListTags", hasEnv && hasOwner);
            } catch (Exception e) {
                ctx.check("OpenSearch ListTags", false, e);
            }

            // 12. RemoveTags
            try {
                os.removeTags(RemoveTagsRequest.builder()
                        .arn(created.arn())
                        .tagKeys("owner")
                        .build());
                ListTagsResponse resp = os.listTags(
                        ListTagsRequest.builder().arn(created.arn()).build());
                boolean ownerGone = resp.tagList().stream()
                        .noneMatch(t -> "owner".equals(t.key()));
                boolean envStays = resp.tagList().stream()
                        .anyMatch(t -> "env".equals(t.key()));
                ctx.check("OpenSearch RemoveTags", ownerGone && envStays);
            } catch (Exception e) {
                ctx.check("OpenSearch RemoveTags", false, e);
            }

            // 13. ListVersions
            try {
                ListVersionsResponse resp = os.listVersions(
                        ListVersionsRequest.builder().build());
                ctx.check("OpenSearch ListVersions",
                        resp.versions() != null && !resp.versions().isEmpty()
                        && resp.versions().contains("OpenSearch_2.11"));
            } catch (Exception e) {
                ctx.check("OpenSearch ListVersions", false, e);
            }

            // 14. GetCompatibleVersions
            try {
                GetCompatibleVersionsResponse resp = os.getCompatibleVersions(
                        GetCompatibleVersionsRequest.builder().build());
                ctx.check("OpenSearch GetCompatibleVersions",
                        resp.compatibleVersions() != null && !resp.compatibleVersions().isEmpty());
            } catch (Exception e) {
                ctx.check("OpenSearch GetCompatibleVersions", false, e);
            }

            // 15. ListInstanceTypeDetails
            try {
                ListInstanceTypeDetailsResponse resp = os.listInstanceTypeDetails(
                        ListInstanceTypeDetailsRequest.builder()
                                .engineVersion("OpenSearch_2.11")
                                .build());
                ctx.check("OpenSearch ListInstanceTypeDetails",
                        resp.instanceTypeDetails() != null && !resp.instanceTypeDetails().isEmpty());
            } catch (Exception e) {
                ctx.check("OpenSearch ListInstanceTypeDetails", false, e);
            }

            // 16. DescribeInstanceTypeLimits
            try {
                DescribeInstanceTypeLimitsResponse resp = os.describeInstanceTypeLimits(
                        DescribeInstanceTypeLimitsRequest.builder()
                                .engineVersion("OpenSearch_2.11")
                                .instanceType(OpenSearchPartitionInstanceType.M5_LARGE_SEARCH)
                                .build());
                ctx.check("OpenSearch DescribeInstanceTypeLimits",
                        resp.limitsByRole() != null && !resp.limitsByRole().isEmpty());
            } catch (Exception e) {
                ctx.check("OpenSearch DescribeInstanceTypeLimits", false, e);
            }

            // 17. DescribeDomainChangeProgress (stub)
            try {
                DescribeDomainChangeProgressResponse resp = os.describeDomainChangeProgress(
                        DescribeDomainChangeProgressRequest.builder().domainName(domainName).build());
                ctx.check("OpenSearch DescribeDomainChangeProgress", resp != null);
            } catch (Exception e) {
                ctx.check("OpenSearch DescribeDomainChangeProgress", false, e);
            }

            // 18. DescribeDomainHealth (stub)
            try {
                DescribeDomainHealthResponse resp = os.describeDomainHealth(
                        DescribeDomainHealthRequest.builder().domainName(domainName).build());
                ctx.check("OpenSearch DescribeDomainHealth",
                        DomainHealth.GREEN == resp.clusterHealth());
            } catch (Exception e) {
                ctx.check("OpenSearch DescribeDomainHealth", false, e);
            }

            // 19. GetUpgradeHistory (stub)
            try {
                GetUpgradeHistoryResponse resp = os.getUpgradeHistory(
                        GetUpgradeHistoryRequest.builder().domainName(domainName).build());
                ctx.check("OpenSearch GetUpgradeHistory",
                        resp.upgradeHistories() != null && resp.upgradeHistories().isEmpty());
            } catch (Exception e) {
                ctx.check("OpenSearch GetUpgradeHistory", false, e);
            }

            // 20. GetUpgradeStatus (stub)
            try {
                GetUpgradeStatusResponse resp = os.getUpgradeStatus(
                        GetUpgradeStatusRequest.builder().domainName(domainName).build());
                ctx.check("OpenSearch GetUpgradeStatus",
                        UpgradeStatus.SUCCEEDED == resp.stepStatus());
            } catch (Exception e) {
                ctx.check("OpenSearch GetUpgradeStatus", false, e);
            }

            // 21. UpgradeDomain (stub)
            try {
                UpgradeDomainResponse resp = os.upgradeDomain(UpgradeDomainRequest.builder()
                        .domainName(domainName)
                        .targetVersion("OpenSearch_2.13")
                        .build());
                ctx.check("OpenSearch UpgradeDomain",
                        domainName.equals(resp.domainName())
                        && "OpenSearch_2.13".equals(resp.targetVersion()));
            } catch (Exception e) {
                ctx.check("OpenSearch UpgradeDomain", false, e);
            }

            // 22. CancelDomainConfigChange (stub)
            try {
                CancelDomainConfigChangeResponse resp = os.cancelDomainConfigChange(
                        CancelDomainConfigChangeRequest.builder().domainName(domainName).build());
                ctx.check("OpenSearch CancelDomainConfigChange",
                        resp.cancelledChangeIds() != null && resp.cancelledChangeIds().isEmpty());
            } catch (Exception e) {
                ctx.check("OpenSearch CancelDomainConfigChange", false, e);
            }

            // 23. StartServiceSoftwareUpdate (stub)
            try {
                StartServiceSoftwareUpdateResponse resp = os.startServiceSoftwareUpdate(
                        StartServiceSoftwareUpdateRequest.builder().domainName(domainName).build());
                ctx.check("OpenSearch StartServiceSoftwareUpdate",
                        resp.serviceSoftwareOptions() != null);
            } catch (Exception e) {
                ctx.check("OpenSearch StartServiceSoftwareUpdate", false, e);
            }

            // 24. CancelServiceSoftwareUpdate (stub)
            try {
                CancelServiceSoftwareUpdateResponse resp = os.cancelServiceSoftwareUpdate(
                        CancelServiceSoftwareUpdateRequest.builder().domainName(domainName).build());
                ctx.check("OpenSearch CancelServiceSoftwareUpdate",
                        resp.serviceSoftwareOptions() != null);
            } catch (Exception e) {
                ctx.check("OpenSearch CancelServiceSoftwareUpdate", false, e);
            }

            // 25. DeleteDomain
            try {
                DeleteDomainResponse resp = os.deleteDomain(
                        DeleteDomainRequest.builder().domainName(domainName).build());
                ctx.check("OpenSearch DeleteDomain",
                        domainName.equals(resp.domainStatus().domainName())
                        && resp.domainStatus().deleted());
            } catch (Exception e) {
                ctx.check("OpenSearch DeleteDomain", false, e);
            }

            // 26. DeleteDomain non-existent → ResourceNotFoundException
            try {
                os.deleteDomain(DeleteDomainRequest.builder().domainName(domainName).build());
                ctx.check("OpenSearch DeleteDomain non-existent fails", false);
            } catch (ResourceNotFoundException e) {
                ctx.check("OpenSearch DeleteDomain non-existent fails", true);
            } catch (Exception e) {
                ctx.check("OpenSearch DeleteDomain non-existent fails", false, e);
            }

        } catch (Exception e) {
            ctx.check("OpenSearch Client", false, e);
        }
    }
}
