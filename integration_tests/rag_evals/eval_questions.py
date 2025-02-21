# flake8: noqa: E501

EVAL_QUESTIONS = [
    "Parties-Answer",
    "Agreement Date-Answer",
    "Effective Date-Answer",
    "Expiration Date-Answer",
    "Governing Law-Answer",
    "Competitive Restriction Exception-Answer",
    "Exclusivity-Answer",
    "Non-Disparagement-Answer",
    "Anti-Assignment-Answer",
    "Minimum Commitment-Answer",
    "License Grant-Answer",
    "Non-Transferable License-Answer",
    "Audit Rights-Answer",
    "Cap On Liability-Answer",
]

EVAL_QUESTIONS += [i.replace("-Answer", "") for i in EVAL_QUESTIONS]


question_mapper = {
    "Non-Disparagement": "Does the contract include a clause prohibiting either party from making negative statements about the other party?",
    "Non-Disparagement-Answer": "Does the contract include a clause prohibiting either party from making negative statements about the other party?",
    "Anti-Assignment": "Does the contract contain a provision that restricts or prohibits the assignment of rights or obligations to a third party?",
    "Anti-Assignment-Answer": "Does the contract contain a provision that restricts or prohibits the assignment of rights or obligations to a third party?",
    "Minimum Commitment": "Is there a minimum commitment required from either party under the terms of the contract?",
    "Minimum Commitment-Answer": "Is there a minimum commitment required from either party under the terms of the contract?",
    "License Grant": "What rights or privileges are granted to the licensee under the contract?",
    "License Grant-Answer": "Are there any rights or privileges granted to the licensee under the contract?",
    "Non-Transferable License": "Does the contract specify that the license granted is non-transferable?",
    "Non-Transferable License-Answer": "Does the contract specify that the license granted is non-transferable?",
    "Audit Rights": "Are there provisions in the contract allowing one party to audit the other party’s compliance with the contract terms?",
    "Audit Rights-Answer": "Are there any provisions in the contract allowing one party to audit the other party’s compliance with the contract terms?",
    "Cap On Liability": "Is there a limit or cap on the amount of liability that one party can incur under the contract?",
    "Cap On Liability-Answer": "Is there a limit or cap on the amount of liability that one party can incur under the contract?",
    "Parties": "Who are the parties in the contract (licensee and licensor)?",
    "Parties-Answer": "Who are the parties in the contract (licensee and licensor)?",
    "Agreement Date": "What is the agreement day of the contract?",
    "Agreement Date-Answer": "What is the agreement day of the contract?",
    "Effective Date": "What is the effective date of the start of contract?",
    "Effective Date-Answer": "What is the effective date of the start of contract?",
    "Expiration Date": "What is the expiration date?",
    "Expiration Date-Answer": "What is the expiration date?",
    "Governing Law": "Governing law clause (State, Country)?",
    "Governing Law-Answer": "Governing law clause (State, Country)?",
    "Competitive Restriction Exception": "Is there Competitive Restriction Exception?",
    "Competitive Restriction Exception-Answer": "Is there Competitive Restriction Exception?",
    "Exclusivity": "Is there exclusivity clause in the contract?",
    "Exclusivity-Answer": "Is there exclusivity clause in the contract?",
}
