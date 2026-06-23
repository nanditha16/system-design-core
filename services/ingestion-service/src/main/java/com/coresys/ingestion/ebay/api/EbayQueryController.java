package com.coresys.ingestion.ebay.api;

import org.springframework.context.annotation.Profile;
import org.springframework.graphql.data.method.annotation.QueryMapping;
import org.springframework.stereotype.Controller;

/** Minimal Query resolver required by GraphQL spec. */
@Profile("ebay")
@Controller
public class EbayQueryController {
    @QueryMapping
    public String ping() { return "ebay-seller-service:UP"; }
}
