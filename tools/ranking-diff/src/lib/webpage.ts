export type SimpleWebpage = {
	title: string;
	url: string;
	snippet: string;
	rankingSignals: Record<string, number>;
};

export const asSimpleWebpage = (webpage: Webpage): SimpleWebpage => {
	return {
		title: webpage.title,
		url: webpage.url,
		snippet: webpage.snippet.text.fragments.map((f) => f.text).join(''),
		rankingSignals: Object.fromEntries(
			Object.entries(webpage.rankingSignals).map(([key, value]) => [
				key,
				value.coefficient * value.value
			])
		)
	};
};

export type Webpage = {
	title: string;
	url: string;
	snippet: Snippet;
	richSnippet: RichSnippet;
	rankingSignals: RankingSignals;
};

export type RankingSignals = Record<string, RankingSignal>;

export type RankingSignal = {
	coefficient: number;
	value: number;
};

export type Snippet = {
	date?: string;
	text: TextSnippet;
};

export type RichSnippet = {
	answers: StackOverflowAnswer[];
	question: StackOverflowQuestion;
	type: 'stackOverflowQA';
};
export type StackOverflowAnswer = {
	accepted: boolean;
	body: CodeOrText[];
	date: string;
	upvotes: number;
	url: string;
};
export type StackOverflowQuestion = {
	body: CodeOrText[];
};
export type TextSnippet = {
	fragments: TextSnippetFragment[];
};
export type TextSnippetFragment = {
	kind: TextSnippetFragmentKind;
	text: string;
};
export type TextSnippetFragmentKind = 'normal' | 'highlighted';

export type CodeOrText =
	| {
			type: 'code';
			value: string;
	  }
	| {
			type: 'text';
			value: string;
	  };
