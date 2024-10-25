<script lang="ts">
	import { onMount } from 'svelte';
	import AddIcon from '~icons/heroicons/plus-circle';
	import DeleteIcon from '~icons/heroicons/trash';
	import type { Category, Query } from '$lib';
	import Navbar from '../components/Navbar.svelte';
	import { get } from 'svelte/store';

	const addQuery = async (query: string) => {
		if (query.trim() === '') {
			return;
		}

		await fetch('/api/queries/insert', {
			method: 'POST',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify({ query })
		});
	};

	const addQuerySubmit = async (e: Event) => {
		e.preventDefault();

		if (!input) {
			return;
		}

		await addQuery(input.value);
		await getQueries();

		input.value = '';
	};

	const getQueries = async () => {
		const res = await fetch('/api/queries');
		queries = await res.json();
	};

	const clearQueries = async () => {
		await fetch('/api/queries/clear', { method: 'POST' });
		await getQueries();
	};

	const deleteQuery = async (id: number) => {
		await fetch('/api/queries/delete', {
			method: 'POST',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify({ id })
		});
		await getQueries();
	};

	const getQueryCategories = async (queryId: number) =>
		(
			await fetch(`/api/queries/categories`, {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({ queryId })
			})
		).json() as Promise<Category[]>;

	const getQueryCategory = async (queryId: number): Promise<Category | undefined> =>
		(await getQueryCategories(queryId))[0];

	const getCategories = async () => {
		const res = await fetch('/api/categories');
		allCategories = await res.json();
	};

	const removeQueryCategory = async (query: Query) => {
		await fetch('/api/queries/remove_category', {
			method: 'POST',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify({ queryId: query.id })
		});
	};

	const updateQueryCategory = async (e: Event, query: Query) => {
		const select = e.target as HTMLSelectElement;

		if (select.value === '') {
			await removeQueryCategory(query);
			return;
		}

		await fetch('/api/queries/add_category', {
			method: 'POST',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify({ queryId: query.id, categoryId: Number(select.value) })
		});
	};

	let queries: Query[] = [];
	let allCategories: Category[] = [];

	let input: HTMLInputElement | null = null;
	onMount(async () => {
		await getQueries();
		await getCategories();
	});
</script>

<Navbar page="queries" />

<div class="flex w-full flex-col items-center">
	<h1 class="text-2xl">Queries</h1>

	<form class="flex gap-x-1" on:submit={addQuerySubmit}>
		<input
			bind:this={input}
			class="rounded border border-slate-400 px-2 py-1"
			placeholder="query"
			type="text"
		/>
		<button type="submit"><AddIcon class="text-green-600" /></button>
	</form>

	<div class="mt-5 flex flex-col space-y-5">
		{#each queries as query}
			<div class="flex items-center justify-between gap-x-5">
				<div class="flex gap-x-2">
					<button on:click={() => deleteQuery(query.id)} class="hover:text-red-600">
						<DeleteIcon class="w-3" />
					</button>
					<p class="w-full text-center">{query.text}</p>
				</div>
				{#if allCategories.length > 0}
					<select
						class="rounded bg-slate-200 px-2 py-1"
						on:change={(e) => updateQueryCategory(e, query)}
					>
						<option value=""></option>

						{#await getQueryCategory(query.id) then selectedCategory}
							{#each allCategories as category}
								<option value={category.id} selected={category.id === selectedCategory?.id}
									>{category.name}</option
								>
							{/each}
						{/await}
					</select>
				{/if}
			</div>
		{/each}
	</div>

	{#if queries.length == 0}
		<p class="text-gray-500">No queries available.</p>
	{:else}
		<button
			on:click={clearQueries}
			class="mt-10 h-8 w-40 rounded bg-red-500 text-white disabled:bg-gray-400">Clear All</button
		>
	{/if}
</div>
