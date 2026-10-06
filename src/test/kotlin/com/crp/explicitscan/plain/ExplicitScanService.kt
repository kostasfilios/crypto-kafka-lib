package com.crp.explicitscan.plain

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.FilterType

/**
 * The shape of ExchangeRequestService, TransactionHistoryService, TransactionHistoryBackOfficeService and
 * TransactionSendMoneyService: `@SpringBootApplication` plus an explicit `@ComponentScan` that covers the lib, without
 * Boot's exclude filters. (The regex only keeps this test module's own classes out of the scan.)
 */
@SpringBootApplication
@ComponentScan(
    basePackages = ["com.crp.system.libs.kafka"],
    excludeFilters = [ComponentScan.Filter(type = FilterType.REGEX, pattern = [".*Test.*", ".*\\.testsupport\\..*"])],
)
class ExplicitScanService
