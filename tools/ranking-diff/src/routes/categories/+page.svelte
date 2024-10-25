<script lang="ts">
	import { onMount } from 'svelte';
	import AddIcon from '~icons/heroicons/plus-circle';
	import DeleteIcon from '~icons/heroicons/trash';
	import type { Category } from '$lib';
	import Navbar from '../components/Navbar.svelte';

	const addCategory = async (name: string) => {
		if (name.trim() === '') {
			return;
		}

		await fetch('/api/categories/new', {
			method: 'POST',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify({ name })
		});
	};

	const addCategorySubmit = async (e: Event) => {
		e.preventDefault();

		if (!input) {
			return;
		}

		await addCategory(input.value);
		await getCategories();

		input.value = '';
	};

	const getCategories = async () => {
		const res = await fetch('/api/categories');
		categories = await res.json();
	};

	const clearCategories = async () => {
		await fetch('/api/categories/clear', { method: 'POST' });
		await getCategories();
	};

	const deleteCategory = async (id: number) => {
		await fetch('/api/categories/delete', {
			method: 'POST',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify({ id })
		});
		await getCategories();
	};

	let categories: Category[] = [];

	let input: HTMLInputElement | null = null;
	onMount(async () => {
		await getCategories();
	});
</script>

<Navbar page="categories" />

<div class="flex w-full flex-col items-center">
	<h1 class="text-2xl">Categories</h1>

	<form class="flex gap-x-1" on:submit={addCategorySubmit}>
		<input
			bind:this={input}
			class="rounded border border-slate-400 px-2 py-1"
			placeholder="category"
			type="text"
		/>
		<button type="submit"><AddIcon class="text-green-600" /></button>
	</form>

	<div class="mt-5 flex flex-col space-y-5">
		{#each categories as category}
			<div class="flex items-center gap-x-2">
				<button on:click={() => deleteCategory(category.id)} class="hover:text-red-600">
					<DeleteIcon class="w-3" />
				</button>
				<p class="w-full text-center">{category.name}</p>
			</div>
		{/each}
	</div>

	{#if categories.length == 0}
		<p class="text-gray-500">No categories available.</p>
	{:else}
		<button
			on:click={clearCategories}
			class="mt-10 h-8 w-40 rounded bg-red-500 text-white disabled:bg-gray-400">Clear All</button
		>
	{/if}
</div>
