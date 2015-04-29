/*******************************************************************************
 * Copyright (c) 2014 itemis AG and others.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * Contributors:
 *     Matthias Wienand (itemis AG) - initial API & implementation
 *
 *******************************************************************************/
package com.typeassimilation.example;

import org.eclipse.gef4.zest.fx.ui.parts.ZestFxUiView;

import com.google.inject.Guice;
import com.google.inject.util.Modules;

public class ZestGraphExampleView extends ZestFxUiView {

	public ZestGraphExampleView() {
		super(Guice
				.createInjector(Modules.override(new ZestGraphExampleModule()).with(new ZestGraphExampleUiModule())));
		setGraph(new ZestGraphExample().createDefaultGraph());
	}

}
