class JobOffer:
    def __init__(
        self, company, title, location, timestamp, telephone, description, email, url
    ):
        self.company = company
        self.title = title
        self.location = location
        self.timestamp = timestamp
        self.telephone = telephone
        self.description = description
        self.email = email
        self.url = url

    def __eq__(self, other):
        if isinstance(other, JobOffer):
            return (
                self.company == other.company
                and self.title == other.title
                and self.location == other.location
                and self.url == other.url
            )
        return False

    def __hash__(self):
        return hash((self.company, self.title, self.location, self.url))

    def __str__(self):
        return (
            f"Job Offer:\n"
            f"Company: {self.company}\n"
            f"Title: {self.title}\n"
            f"Location: {self.location}\n"
            f"Timestamp: {self.timestamp}\n"
            f"Telephone: {self.telephone}\n"
            f"Description: {self.description}"
            f"\nEmail: {self.email}\n"
            f"URL: {self.url}"
        )
