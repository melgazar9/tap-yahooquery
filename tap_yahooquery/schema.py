from singer_sdk import typing as th

CUSTOM_JSON_SCHEMA = {
    "additionalProperties": True,
    "description": "Custom JSON typing.",
    "type": ["object", "null"],
}


ALL_FINANCIAL_DATA_SCHEMA = th.PropertiesList(
    # Core Identifiers
    th.Property("ticker", th.StringType, required=True, description="Stock ticker"),
    th.Property(
        "as_of_date",
        th.DateType,
        required=True,
        description="Date of the financial data",
    ),
    th.Property(
        "period_type",
        th.StringType,
        description="Period type (quarterly, annual, etc.)",
    ),
    th.Property(
        "currency_code",
        th.StringType,
        description="Currency code for financial figures",
    ),
    # Balance Sheet - Current Assets
    th.Property(
        "cash_and_cash_equivalents",
        th.NumberType,
        description="Cash and cash equivalents",
    ),
    th.Property(
        "cash_cash_equivalents_and_short_term_investments",
        th.NumberType,
        description="Cash, cash equivalents and short-term investments",
    ),
    th.Property("cash_equivalents", th.NumberType, description="Cash equivalents"),
    th.Property("cash_financial", th.NumberType, description="Financial cash"),
    th.Property(
        "accounts_receivable", th.NumberType, description="Accounts receivable"
    ),
    th.Property("receivables", th.NumberType, description="Total receivables"),
    th.Property("other_receivables", th.NumberType, description="Other receivables"),
    th.Property("inventory", th.NumberType, description="Inventory"),
    th.Property(
        "other_current_assets", th.NumberType, description="Other current assets"
    ),
    th.Property("current_assets", th.NumberType, description="Total current assets"),
    # Balance Sheet - Non-Current Assets
    th.Property(
        "gross_ppe", th.NumberType, description="Gross property, plant and equipment"
    ),
    th.Property(
        "net_ppe", th.NumberType, description="Net property, plant and equipment"
    ),
    th.Property(
        "accumulated_depreciation",
        th.NumberType,
        description="Accumulated depreciation",
    ),
    th.Property(
        "land_and_improvements", th.NumberType, description="Land and improvements"
    ),
    th.Property(
        "machinery_furniture_equipment",
        th.NumberType,
        description="Machinery, furniture and equipment",
    ),
    th.Property("properties", th.NumberType, description="Properties"),
    th.Property("other_properties", th.NumberType, description="Other properties"),
    th.Property(
        "investments_and_advances",
        th.NumberType,
        description="Investments and advances",
    ),
    th.Property("other_investments", th.NumberType, description="Other investments"),
    th.Property(
        "other_short_term_investments",
        th.NumberType,
        description="Other short-term investments",
    ),
    th.Property(
        "available_for_sale_securities",
        th.NumberType,
        description="Available for sale securities",
    ),
    th.Property(
        "non_current_deferred_assets",
        th.NumberType,
        description="Non-current deferred assets",
    ),
    th.Property(
        "non_current_deferred_taxes_assets",
        th.NumberType,
        description="Non-current deferred tax assets",
    ),
    th.Property(
        "other_non_current_assets",
        th.NumberType,
        description="Other non-current assets",
    ),
    th.Property(
        "total_non_current_assets",
        th.NumberType,
        description="Total non-current assets",
    ),
    th.Property("total_assets", th.NumberType, description="Total assets"),
    # Balance Sheet - Current Liabilities
    th.Property("accounts_payable", th.NumberType, description="Accounts payable"),
    th.Property("payables", th.NumberType, description="Total payables"),
    th.Property(
        "payables_and_accrued_expenses",
        th.NumberType,
        description="Payables and accrued expenses",
    ),
    th.Property("income_tax_payable", th.NumberType, description="Income tax payable"),
    th.Property("current_debt", th.NumberType, description="Current debt"),
    th.Property(
        "current_capital_lease_obligation",
        th.NumberType,
        description="Current capital lease obligation",
    ),
    th.Property(
        "current_debt_and_capital_lease_obligation",
        th.NumberType,
        description="Current debt and capital lease obligation",
    ),
    th.Property("commercial_paper", th.NumberType, description="Commercial paper"),
    th.Property(
        "other_current_borrowings",
        th.NumberType,
        description="Other current borrowings",
    ),
    th.Property(
        "current_deferred_liabilities",
        th.NumberType,
        description="Current deferred liabilities",
    ),
    th.Property(
        "current_deferred_revenue",
        th.NumberType,
        description="Current deferred revenue",
    ),
    th.Property(
        "other_current_liabilities",
        th.NumberType,
        description="Other current liabilities",
    ),
    th.Property(
        "current_liabilities", th.NumberType, description="Total current liabilities"
    ),
    # Balance Sheet - Non-Current Liabilities
    th.Property("long_term_debt", th.NumberType, description="Long-term debt"),
    th.Property(
        "long_term_capital_lease_obligation",
        th.NumberType,
        description="Long-term capital lease obligation",
    ),
    th.Property(
        "long_term_debt_and_capital_lease_obligation",
        th.NumberType,
        description="Long-term debt and capital lease obligation",
    ),
    th.Property(
        "capital_lease_obligations",
        th.NumberType,
        description="Capital lease obligations",
    ),
    th.Property("leases", th.NumberType, description="Leases"),
    th.Property("deferred_tax", th.NumberType, description="Deferred tax"),
    th.Property(
        "deferred_income_tax", th.NumberType, description="Deferred income tax"
    ),
    th.Property(
        "tradeand_other_payables_non_current",
        th.NumberType,
        description="Trade and other payables non-current",
    ),
    th.Property(
        "other_non_current_liabilities",
        th.NumberType,
        description="Other non-current liabilities",
    ),
    th.Property(
        "total_non_current_liabilities_net_minority_interest",
        th.NumberType,
        description="Total non-current liabilities net minority interest",
    ),
    th.Property(
        "total_liabilities_net_minority_interest",
        th.NumberType,
        description="Total liabilities net minority interest",
    ),
    th.Property("total_debt", th.NumberType, description="Total debt"),
    th.Property("total_tax_payable", th.NumberType, description="Total tax payable"),
    # Balance Sheet - Equity
    th.Property("common_stock", th.NumberType, description="Common stock"),
    th.Property("capital_stock", th.NumberType, description="Capital stock"),
    th.Property("retained_earnings", th.NumberType, description="Retained earnings"),
    th.Property(
        "common_stock_equity", th.NumberType, description="Common stock equity"
    ),
    th.Property(
        "stockholders_equity", th.NumberType, description="Stockholders equity"
    ),
    th.Property(
        "total_equity_gross_minority_interest",
        th.NumberType,
        description="Total equity gross minority interest",
    ),
    th.Property(
        "gains_losses_not_affecting_retained_earnings",
        th.NumberType,
        description="Gains/losses not affecting retained earnings",
    ),
    th.Property(
        "other_equity_adjustments",
        th.NumberType,
        description="Other equity adjustments",
    ),
    # Income Statement - Revenue
    th.Property("total_revenue", th.NumberType, description="Total revenue"),
    th.Property("operating_revenue", th.NumberType, description="Operating revenue"),
    # Income Statement - Costs and Expenses
    th.Property("cost_of_revenue", th.NumberType, description="Cost of revenue"),
    th.Property(
        "reconciled_cost_of_revenue",
        th.NumberType,
        description="Reconciled cost of revenue",
    ),
    th.Property("gross_profit", th.NumberType, description="Gross profit"),
    th.Property("operating_expense", th.NumberType, description="Operating expense"),
    th.Property(
        "selling_general_and_administration",
        th.NumberType,
        description="Selling, general and administration",
    ),
    th.Property(
        "research_and_development",
        th.NumberType,
        description="Research and development",
    ),
    th.Property(
        "depreciation_and_amortization",
        th.NumberType,
        description="Depreciation and amortization",
    ),
    th.Property(
        "depreciation_amortization_depletion",
        th.NumberType,
        description="Depreciation, amortization and depletion",
    ),
    th.Property(
        "reconciled_depreciation", th.NumberType, description="Reconciled depreciation"
    ),
    th.Property("total_expenses", th.NumberType, description="Total expenses"),
    # Income Statement - Operating Income
    th.Property("operating_income", th.NumberType, description="Operating income"),
    th.Property(
        "total_operating_income_as_reported",
        th.NumberType,
        description="Total operating income as reported",
    ),
    th.Property(
        "ebit", th.NumberType, description="EBIT - Earnings before interest and taxes"
    ),
    th.Property(
        "ebitda",
        th.NumberType,
        description="EBITDA - Earnings before interest, taxes, depreciation and amortization",
    ),
    th.Property("normalized_ebitda", th.NumberType, description="Normalized EBITDA"),
    # Income Statement - Non-Operating Items
    th.Property("interest_income", th.NumberType, description="Interest income"),
    th.Property(
        "interest_income_non_operating",
        th.NumberType,
        description="Interest income non-operating",
    ),
    th.Property(
        "net_interest_income", th.NumberType, description="Net interest income"
    ),
    th.Property("interest_expense", th.NumberType, description="Interest expense"),
    th.Property(
        "interest_expense_non_operating",
        th.NumberType,
        description="Interest expense non-operating",
    ),
    th.Property(
        "net_non_operating_interest_income_expense",
        th.NumberType,
        description="Net non-operating interest income/expense",
    ),
    th.Property(
        "other_income_expense", th.NumberType, description="Other income/expense"
    ),
    th.Property(
        "other_non_operating_income_expenses",
        th.NumberType,
        description="Other non-operating income/expenses",
    ),
    th.Property(
        "net_other_financing_charges",
        th.NumberType,
        description="Net other financing charges",
    ),
    # Income Statement - Pre-tax and Taxes
    th.Property("pretax_income", th.NumberType, description="Pretax income"),
    th.Property("tax_provision", th.NumberType, description="Tax provision"),
    th.Property(
        "tax_rate_for_calcs", th.NumberType, description="Tax rate for calculations"
    ),
    th.Property(
        "tax_effect_of_unusual_items",
        th.NumberType,
        description="Tax effect of unusual items",
    ),
    # Income Statement - Net Income
    th.Property("net_income", th.NumberType, description="Net income"),
    th.Property(
        "net_income_common_stockholders",
        th.NumberType,
        description="Net income common stockholders",
    ),
    th.Property(
        "net_income_continuous_operations",
        th.NumberType,
        description="Net income continuous operations",
    ),
    th.Property(
        "net_income_from_continuing_operations",
        th.NumberType,
        description="Net income from continuing operations",
    ),
    th.Property(
        "net_income_from_continuing_operation_net_minority_interest",
        th.NumberType,
        description="Net income from continuing operation net minority interest",
    ),
    th.Property(
        "net_income_from_continuing_and_discontinued_operation",
        th.NumberType,
        description="Net income from continuing and discontinued operation",
    ),
    th.Property(
        "net_income_including_noncontrolling_interests",
        th.NumberType,
        description="Net income including noncontrolling interests",
    ),
    th.Property("normalized_income", th.NumberType, description="Normalized income"),
    # Earnings Per Share
    th.Property("basic_eps", th.NumberType, description="Basic earnings per share"),
    th.Property("diluted_eps", th.NumberType, description="Diluted earnings per share"),
    th.Property(
        "basic_average_shares", th.NumberType, description="Basic average shares"
    ),
    th.Property(
        "diluted_average_shares", th.NumberType, description="Diluted average shares"
    ),
    th.Property(
        "diluted_ni_avail_to_common_stock_holders",
        th.NumberType,
        description="Diluted NI available to common stockholders",
    ),
    # Share Information
    th.Property(
        "ordinary_shares_number", th.NumberType, description="Ordinary shares number"
    ),
    th.Property("share_issued", th.NumberType, description="Shares issued"),
    th.Property(
        "treasury_shares_number", th.NumberType, description="Treasury shares number"
    ),
    # Cash Flow Statement - Operating Activities
    th.Property(
        "operating_cash_flow", th.NumberType, description="Operating cash flow"
    ),
    th.Property(
        "cash_flow_from_continuing_operating_activities",
        th.NumberType,
        description="Cash flow from continuing operating activities",
    ),
    th.Property(
        "change_in_working_capital",
        th.NumberType,
        description="Change in working capital",
    ),
    th.Property(
        "change_in_other_working_capital",
        th.NumberType,
        description="Change in other working capital",
    ),
    th.Property(
        "changes_in_account_receivables",
        th.NumberType,
        description="Changes in account receivables",
    ),
    th.Property(
        "change_in_receivables", th.NumberType, description="Change in receivables"
    ),
    th.Property(
        "change_in_inventory", th.NumberType, description="Change in inventory"
    ),
    th.Property(
        "change_in_account_payable",
        th.NumberType,
        description="Change in account payable",
    ),
    th.Property("change_in_payable", th.NumberType, description="Change in payable"),
    th.Property(
        "change_in_payables_and_accrued_expense",
        th.NumberType,
        description="Change in payables and accrued expense",
    ),
    th.Property(
        "change_in_other_current_assets",
        th.NumberType,
        description="Change in other current assets",
    ),
    th.Property(
        "change_in_other_current_liabilities",
        th.NumberType,
        description="Change in other current liabilities",
    ),
    th.Property(
        "stock_based_compensation",
        th.NumberType,
        description="Stock-based compensation",
    ),
    th.Property(
        "other_non_cash_items", th.NumberType, description="Other non-cash items"
    ),
    # Cash Flow Statement - Investing Activities
    th.Property(
        "investing_cash_flow", th.NumberType, description="Investing cash flow"
    ),
    th.Property(
        "cash_flow_from_continuing_investing_activities",
        th.NumberType,
        description="Cash flow from continuing investing activities",
    ),
    th.Property(
        "capital_expenditure", th.NumberType, description="Capital expenditure"
    ),
    th.Property("purchase_of_ppe", th.NumberType, description="Purchase of PP&E"),
    th.Property(
        "net_ppe_purchase_and_sale",
        th.NumberType,
        description="Net PP&E purchase and sale",
    ),
    th.Property(
        "purchase_of_investment", th.NumberType, description="Purchase of investment"
    ),
    th.Property("sale_of_investment", th.NumberType, description="Sale of investment"),
    th.Property(
        "net_investment_purchase_and_sale",
        th.NumberType,
        description="Net investment purchase and sale",
    ),
    th.Property(
        "investment_in_financial_assets",
        th.NumberType,
        description="Investment in financial assets",
    ),
    th.Property(
        "purchase_of_business", th.NumberType, description="Purchase of business"
    ),
    th.Property(
        "net_business_purchase_and_sale",
        th.NumberType,
        description="Net business purchase and sale",
    ),
    th.Property(
        "net_other_investing_changes",
        th.NumberType,
        description="Net other investing changes",
    ),
    # Cash Flow Statement - Financing Activities
    th.Property(
        "financing_cash_flow", th.NumberType, description="Financing cash flow"
    ),
    th.Property(
        "cash_flow_from_continuing_financing_activities",
        th.NumberType,
        description="Cash flow from continuing financing activities",
    ),
    th.Property(
        "common_stock_issuance", th.NumberType, description="Common stock issuance"
    ),
    th.Property(
        "issuance_of_capital_stock",
        th.NumberType,
        description="Issuance of capital stock",
    ),
    th.Property(
        "net_common_stock_issuance",
        th.NumberType,
        description="Net common stock issuance",
    ),
    th.Property(
        "common_stock_payments", th.NumberType, description="Common stock payments"
    ),
    th.Property(
        "repurchase_of_capital_stock",
        th.NumberType,
        description="Repurchase of capital stock",
    ),
    th.Property(
        "cash_dividends_paid", th.NumberType, description="Cash dividends paid"
    ),
    th.Property(
        "common_stock_dividend_paid",
        th.NumberType,
        description="Common stock dividend paid",
    ),
    th.Property("issuance_of_debt", th.NumberType, description="Issuance of debt"),
    th.Property(
        "long_term_debt_issuance", th.NumberType, description="Long-term debt issuance"
    ),
    th.Property(
        "net_long_term_debt_issuance",
        th.NumberType,
        description="Net long-term debt issuance",
    ),
    th.Property(
        "net_short_term_debt_issuance",
        th.NumberType,
        description="Net short-term debt issuance",
    ),
    th.Property(
        "long_term_debt_payments", th.NumberType, description="Long-term debt payments"
    ),
    th.Property("repayment_of_debt", th.NumberType, description="Repayment of debt"),
    th.Property(
        "net_issuance_payments_of_debt",
        th.NumberType,
        description="Net issuance/payments of debt",
    ),
    # Cash Flow Statement - Cash Position
    th.Property(
        "beginning_cash_position", th.NumberType, description="Beginning cash position"
    ),
    th.Property("end_cash_position", th.NumberType, description="End cash position"),
    th.Property("changes_in_cash", th.NumberType, description="Changes in cash"),
    th.Property(
        "change_in_cash_supplemental_as_reported",
        th.NumberType,
        description="Change in cash supplemental as reported",
    ),
    th.Property("free_cash_flow", th.NumberType, description="Free cash flow"),
    # Supplemental Cash Flow Data
    th.Property(
        "income_tax_paid_supplemental_data",
        th.NumberType,
        description="Income tax paid supplemental data",
    ),
    th.Property(
        "interest_paid_supplemental_data",
        th.NumberType,
        description="Interest paid supplemental data",
    ),
    # Financial Ratios and Metrics
    th.Property("working_capital", th.NumberType, description="Working capital"),
    th.Property("invested_capital", th.NumberType, description="Invested capital"),
    th.Property(
        "tangible_book_value", th.NumberType, description="Tangible book value"
    ),
    th.Property(
        "net_tangible_assets", th.NumberType, description="Net tangible assets"
    ),
    th.Property("net_debt", th.NumberType, description="Net debt"),
    th.Property(
        "total_capitalization", th.NumberType, description="Total capitalization"
    ),
    th.Property("market_cap", th.NumberType, description="Market capitalization"),
    th.Property("enterprise_value", th.NumberType, description="Enterprise value"),
    th.Property(
        "enterprises_value_ebitda_ratio",
        th.NumberType,
        description="Enterprise value to EBITDA ratio",
    ),
    th.Property(
        "enterprises_value_revenue_ratio",
        th.NumberType,
        description="Enterprise value to revenue ratio",
    ),
    # Others
    th.Property("special_income_charges", th.NumberType),
    th.Property("depreciation_and_amortization_in_income_statement", th.NumberType),
    th.Property("depreciation_amortization_depletion_income_statement", th.NumberType),
    th.Property("selling_and_marketing_expense", th.NumberType),
    th.Property("gain_on_sale_of_ppe", th.NumberType),
    th.Property("general_and_administrative_expense", th.NumberType),
    th.Property("other_g_and_a", th.NumberType),
    th.Property("write_off", th.NumberType),
    th.Property("amortization_of_intangibles_income_statement", th.NumberType),
    th.Property("other_operating_expenses", th.NumberType),
    th.Property("total_unusual_items_excluding_goodwill", th.NumberType),
    th.Property("amortization", th.NumberType),
    th.Property("total_unusual_items", th.NumberType),
    th.Property("minority_interests", th.NumberType),
    th.Property("gain_on_sale_of_security", th.NumberType),
    th.Property("other_under_preferred_stock_dividend", th.NumberType),
    th.Property("total_other_finance_cost", th.NumberType),
    th.Property("depreciation_income_statement", th.NumberType),
    th.Property("impairment_of_capital_assets", th.NumberType),
    th.Property("other_special_charges", th.NumberType),
    th.Property("rent_and_landing_fees", th.NumberType),
    th.Property("rent_expense_supplemental", th.NumberType),
    th.Property("restructuring_and_merger_n_acquisition", th.NumberType),
    th.Property("net_income_discontinuous_operations", th.NumberType),
    th.Property("gain_on_sale_of_business", th.NumberType),
    th.Property("salaries_and_wages", th.NumberType),
    th.Property("earnings_from_equity_interest_net_of_tax", th.NumberType),
    th.Property("average_dilution_earnings", th.NumberType),
    th.Property("preferred_stock_dividends", th.NumberType),
    th.Property("insurance_and_claims", th.NumberType),
    th.Property("earnings_from_equity_interest", th.NumberType),
    th.Property("other_taxes", th.NumberType),
    th.Property("net_income_extraordinary", th.NumberType),
    th.Property("provision_for_doubtful_accounts", th.NumberType),
    th.Property("securities_amortization", th.NumberType),
    th.Property("excise_taxes", th.NumberType),
    th.Property("net_income_from_tax_loss_carryforward", th.NumberType),
    th.Property("depletion_income_statement", th.NumberType),
    th.Property("depreciation", th.NumberType),
    th.Property("policyholder_benefits_gross", th.NumberType),
    th.Property("net_policyholder_benefits_and_claims", th.NumberType),
    th.Property("policyholder_benefits_ceded", th.NumberType),
    th.Property("loss_adjustment_expense", th.NumberType),
    th.Property("other_non_interest_expense", th.NumberType),
    th.Property("occupancy_and_equipment", th.NumberType),
    th.Property("professional_expense_and_contract_services_expense", th.NumberType),
).to_dict()

INCOME_STMT_SCHEMA = th.PropertiesList(
    th.Property("as_of_date", th.DateType, required=True),
    th.Property("ticker", th.StringType, required=True),
    th.Property("basic_average_shares", th.NumberType),
    th.Property("basic_eps", th.NumberType),
    th.Property("cost_of_revenue", th.NumberType),
    th.Property("diluted_average_shares", th.NumberType),
    th.Property("diluted_eps", th.NumberType),
    th.Property("diluted_ni_avail_to_common_stock_holders", th.NumberType),
    th.Property("ebit", th.NumberType),
    th.Property("ebitda", th.NumberType),
    th.Property("gross_profit", th.NumberType),
    th.Property("interest_expense", th.NumberType),
    th.Property("interest_expense_non_operating", th.NumberType),
    th.Property("interest_income_non_operating", th.NumberType),
    th.Property("net_income", th.NumberType),
    th.Property("net_income_common_stockholders", th.NumberType),
    th.Property("net_income_continuous_operations", th.NumberType),
    th.Property("net_income_from_continuing_and_discontinued_operation", th.NumberType),
    th.Property(
        "net_income_from_continuing_operation_net_minority_interest", th.NumberType
    ),
    th.Property("net_income_including_noncontrolling_interests", th.NumberType),
    th.Property("net_interest_income", th.NumberType),
    th.Property("net_non_operating_interest_income_expense", th.NumberType),
    th.Property("normalized_ebitda", th.NumberType),
    th.Property("normalized_income", th.NumberType),
    th.Property("operating_expense", th.NumberType),
    th.Property("operating_income", th.NumberType),
    th.Property("operating_revenue", th.NumberType),
    th.Property("other_income_expense", th.NumberType),
    th.Property("other_non_operating_income_expenses", th.NumberType),
    th.Property("pretax_income", th.NumberType),
    th.Property("reconciled_cost_of_revenue", th.NumberType),
    th.Property("reconciled_depreciation", th.NumberType),
    th.Property("research_and_development", th.NumberType),
    th.Property("selling_general_and_administration", th.NumberType),
    th.Property("tax_effect_of_unusual_items", th.NumberType),
    th.Property("tax_provision", th.NumberType),
    th.Property("tax_rate_for_calcs", th.NumberType),
    th.Property("total_expenses", th.NumberType),
    th.Property("total_operating_income_as_reported", th.NumberType),
    th.Property("total_revenue", th.NumberType),
    th.Property("interest_income", th.NumberType),
    th.Property("special_income_charges", th.NumberType),
    th.Property("depreciation_and_amortization_in_income_statement", th.NumberType),
    th.Property("depreciation_amortization_depletion_income_statement", th.NumberType),
    th.Property("selling_and_marketing_expense", th.NumberType),
    th.Property("gain_on_sale_of_ppe", th.NumberType),
    th.Property("general_and_administrative_expense", th.NumberType),
    th.Property("other_g_and_a", th.NumberType),
    th.Property("write_off", th.NumberType),
    th.Property("amortization_of_intangibles_income_statement", th.NumberType),
    th.Property("other_operating_expenses", th.NumberType),
    th.Property("total_unusual_items_excluding_goodwill", th.NumberType),
    th.Property("amortization", th.NumberType),
    th.Property("total_unusual_items", th.NumberType),
    th.Property("minority_interests", th.NumberType),
    th.Property("gain_on_sale_of_security", th.NumberType),
    th.Property("other_under_preferred_stock_dividend", th.NumberType),
    th.Property("total_other_finance_cost", th.NumberType),
    th.Property("depreciation_income_statement", th.NumberType),
    th.Property("impairment_of_capital_assets", th.NumberType),
    th.Property("other_special_charges", th.NumberType),
    th.Property("rent_and_landing_fees", th.NumberType),
    th.Property("rent_expense_supplemental", th.NumberType),
    th.Property("restructuring_and_merger_n_acquisition", th.NumberType),
    th.Property("net_income_discontinuous_operations", th.NumberType),
    th.Property("gain_on_sale_of_business", th.NumberType),
    th.Property("salaries_and_wages", th.NumberType),
    th.Property("earnings_from_equity_interest_net_of_tax", th.NumberType),
    th.Property("average_dilution_earnings", th.NumberType),
    th.Property("preferred_stock_dividends", th.NumberType),
    th.Property("insurance_and_claims", th.NumberType),
    th.Property("earnings_from_equity_interest", th.NumberType),
    th.Property("other_taxes", th.NumberType),
    th.Property("net_income_extraordinary", th.NumberType),
    th.Property("provision_for_doubtful_accounts", th.NumberType),
    th.Property("securities_amortization", th.NumberType),
    th.Property("excise_taxes", th.NumberType),
    th.Property("net_income_from_tax_loss_carryforward", th.NumberType),
    th.Property("depletion_income_statement", th.NumberType),
    th.Property("depreciation", th.NumberType),
    th.Property("policyholder_benefits_gross", th.NumberType),
    th.Property("net_policyholder_benefits_and_claims", th.NumberType),
    th.Property("policyholder_benefits_ceded", th.NumberType),
    th.Property("loss_adjustment_expense", th.NumberType),
    th.Property("other_non_interest_expense", th.NumberType),
    th.Property("occupancy_and_equipment", th.NumberType),
    th.Property("professional_expense_and_contract_services_expense", th.NumberType),
    th.Property("period_type", th.StringType),
    th.Property("currency_code", th.StringType),
).to_dict()
