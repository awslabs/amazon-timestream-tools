# Security Policy

## Reporting a Vulnerability

We take the security of all tools seriously. If you believe you have found a security vulnerability, please report it to us as described below.

### How to Report

**Please do not report security vulnerabilities through public GitHub issues.**

Instead, please report them by sending an email to the AWS Security team. See [AWS Vulnerability Reporting](https://aws.amazon.com/security/vulnerability-reporting/) for details.

### What to Include

Please include the following information in your report:

- Type of issue (e.g., buffer overflow, SQL injection, cross-site scripting, etc.)
- Full paths of source file(s) related to the manifestation of the issue
- The location of the affected source code (tag/branch/commit or direct URL)
- Any special configuration required to reproduce the issue
- Step-by-step instructions to reproduce the issue
- Proof-of-concept or exploit code (if possible)
- Impact of the issue, including how an attacker might exploit it

## Security Best Practices

When using any of the tools:

1. **Environment Variables**: Never commit sensitive environment variables. Use `.env` files (excluded from git) or secure secret management.

2. **Use the most restrictive IAM policy permissions possible.** For all tools, use IAM policies with least-privilege.

## Dependency Management

We actively monitor and update dependencies to address security vulnerabilities:

- **Dependabot**: Automated dependency updates via GitHub Dependabot
- **Regular Audits**: Periodic review of tool dependency trees for security issues

## Security Updates

Security updates are made on a tool-by-tool basis.

## License

This project is licensed under the MIT-0 License. See [LICENSE](LICENSE) for details.
