# Contributing

Bug reports, ideas and pull requests are welcome. Before you open a pull request, please read the terms below: by submitting a contribution you accept them.

## Before you submit

```bash
cargo clippy --all-targets   # must stay warning-free
cargo test
```

Behavioural changes to the connection runner need a scenario in `tests/client.rs`. Public API changes must be reflected in `README.md`, `CHANGELOG.md` and `HA_CORE_CLIENT_SKILL.md`. `ARCHITECTURE.md` describes the internals.

## Contribution terms

"Contribution" means any code, documentation or other material you submit to this project (pull request, patch, suggested change, code in an issue or comment). "Maintainer" means the owner of this repository (GitHub user `pavver`) and their successors and assigns.

By submitting a Contribution you agree that:

1. **You have the right to submit it.** It is your own original work, or you are otherwise entitled to submit it under these terms (for example, your employer allows it). If it contains third-party material, you say so and name its license.
2. **License to the Maintainer.** You grant the Maintainer a perpetual, worldwide, non-exclusive, royalty-free, irrevocable license to use, copy, modify, publish, distribute, sublicense and relicense your Contribution, alone or as part of the project, under any terms the Maintainer chooses, including licenses other than MPL-2.0 and proprietary licenses.
3. **Future license changes.** The project's license may change in the future. Your Contribution may be distributed under the new license without asking for your further consent.
4. **Patents.** You grant the Maintainer and every recipient of the project a perpetual, worldwide, non-exclusive, royalty-free, irrevocable patent license to make, use, sell, offer for sale, import and otherwise transfer your Contribution, alone or combined with the project.
5. **No claims.** You will not bring any claim against the Maintainer or recipients of the project over the use, modification, distribution or relicensing of your Contribution within the scope of these terms. To the extent permitted by applicable law, you waive, or agree not to assert, any moral rights in your Contribution.
6. **No obligations for the Maintainer.** The Maintainer is not required to accept, use or keep your Contribution, or to pay for it. Attribution is kept in the git history; no other attribution is owed.
7. **No warranty.** You provide your Contribution "as is", without warranties of any kind.

You keep the copyright in your Contribution and can use it in any other way you like. Contributions are distributed to everyone else under the project's license at the time (currently [MPL-2.0](LICENSE)).

If you do not agree with these terms, please do not submit a Contribution. Opening an issue that describes a bug or an idea without code is always fine.
